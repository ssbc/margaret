package offset // import "cryptoscope.co/go/margaret/offset"

import (
	"context"
  "encoding/binary"
  "io"
	"sync"

	"cryptoscope.co/go/luigi"
	"cryptoscope.co/go/margaret"
	"cryptoscope.co/go/margaret/codec"

  "github.com/pkg/errors"
)

type offsetQuery struct {
  l sync.Mutex
	log *offsetLog
  codec codec.Codec

	nextSeq, lt margaret.Seq

	limit int
	live  bool
}

func (qry *offsetQuery) Gt(s margaret.Seq) error {
	if qry.nextSeq > margaret.SeqEmpty {
		return errors.Errorf("lower bound already set")
	}

  qry.nextSeq = s+1
  return nil
}

func (qry *offsetQuery) Gte(s margaret.Seq) error {
	if qry.nextSeq > margaret.SeqEmpty {
		return errors.Errorf("lower bound already set")
	}

  qry.nextSeq = s
  return nil
}

func (qry *offsetQuery) Lt(s margaret.Seq) error {
	if qry.lt != margaret.SeqEmpty {
		return errors.Errorf("upper bound already set")
	}

	qry.lt = s
	return nil
}

func (qry *offsetQuery) Lte(s margaret.Seq) error {
	if qry.lt != margaret.SeqEmpty {
		return errors.Errorf("upper bound already set")
	}

	qry.lt = s+1
	return nil
}

func (qry *offsetQuery) Limit(n int) error {
	qry.limit = n
	return nil
}

func (qry *offsetQuery) Live(live bool) error {
	qry.live = live
	return nil
}

func (qry *offsetQuery) Next(ctx context.Context) (interface{}, error) {
  qry.l.Lock()
  defer qry.l.Unlock()

	if qry.limit == 0 {
		return nil, luigi.EOS{}
	}
	qry.limit--

  if qry.nextSeq == margaret.SeqEmpty {
    qry.nextSeq = 0
  }

	qry.log.l.Lock()
	defer qry.log.l.Unlock()

  // only seek to eof if file not empty
  fi, err := qry.log.f.Stat()
  if err != nil {
    return nil, errors.Wrap(err, "stat error")
  }

  seekTo := int64(qry.nextSeq) * qry.log.blocksize

  if size:= fi.Size(); size < seekTo + qry.log.blocksize {
    if !qry.live {
      return nil, luigi.EOS{}
    }

    wait := make(chan struct{})
    var cancel func()
    cancel = qry.log.seq.Register(luigi.FuncSink(
      func(ctx context.Context, v interface{}, doClose bool) error {
        if doClose {
          return luigi.EOS{}
        }
        if v.(margaret.Seq) >= qry.nextSeq {
          close(wait)
          cancel()
        }

        return nil
      }))

    err := func() error {
      qry.log.l.Unlock()
      defer qry.log.l.Lock()

      select {
      case <-wait:
      case <-ctx.Done():
        return ctx.Err()
      }
      return nil
    }()
    if err != nil {
      return nil, err
    }
  }

  _, err = qry.log.f.Seek(int64(qry.nextSeq) * qry.log.blocksize, io.SeekStart)
  if err != nil {
    return nil, errors.Wrap(err, "seek failed")
  }

	if qry.lt != margaret.SeqEmpty && !(qry.nextSeq < qry.lt) {
		return nil, luigi.EOS{}
	}

  out := make([]byte, qry.log.blocksize)
  _, err = qry.log.f.Read(out)
  if err == io.EOF {
    return nil, luigi.EOS{}
  } else if err != nil {
    return nil, errors.Wrap(err, "error reading block")
  }

  l := binary.BigEndian.Uint32(out)
  v, err := qry.codec.Unmarshal(out[4:l+4])
  if err != nil {
    return nil, errors.Wrap(err, "error unmarshaling block")
  }

  qry.nextSeq++

  return v, nil
}
