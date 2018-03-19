package offset // import "cryptoscope.co/go/margaret/offset"

import (
	"context"
	"io"
	"os"
	"sync"

	"cryptoscope.co/go/luigi"
	"cryptoscope.co/go/margaret"
	"cryptoscope.co/go/margaret/codec"

	"github.com/pkg/errors"
)

const defaultFrameSize = 4096

type Framing interface {
	margaret.Framing

	FrameSize() int64
}

type offsetLog struct {
	l sync.Mutex
	f *os.File

	seq     luigi.Observable
	codec   codec.Codec
	framing Framing
}

func NewOffsetLog(f *os.File, framing Framing, cdc codec.Codec) margaret.Log {
	log := &offsetLog{
		f:       f,
		seq:     luigi.NewObservable(margaret.SeqEmpty),
		framing: framing,
		codec:   cdc,
	}

	return log
}

func (log *offsetLog) Seq() luigi.Observable {
	return log.seq
}

func (log *offsetLog) Get(s margaret.Seq) (interface{}, error) {
	q, err := log.Query(margaret.Limit(1), margaret.Gte(s))
	if err != nil {
		return nil, errors.Wrap(err, "error constructing single-value query")
	}

	return q.Next(context.TODO())
}

func (log *offsetLog) Query(specs ...margaret.QuerySpec) (luigi.Source, error) {
	log.l.Lock()
	defer log.l.Unlock()

	qry := &offsetQuery{
		log:   log,
		codec: log.codec,

		nextSeq: margaret.SeqEmpty,
		lt:      margaret.SeqEmpty,

		limit: -1, //i.e. no limit
	}

	for _, spec := range specs {
		err := spec(qry)
		if err != nil {
			return nil, err
		}
	}

	return qry, nil
}

func (log *offsetLog) Append(v interface{}) error {
	data, err := log.codec.Marshal(v)
	if err != nil {
		return errors.Wrap(err, "error marshaling value")
	}

	log.l.Lock()
	defer log.l.Unlock()

	// only seek to eof if file not empty
	fi, err := log.f.Stat()
	if err != nil {
		return errors.Wrap(err, "stat error")
	}

	if fi.Size() > 0 {
		_, err = log.f.Seek(0, io.SeekEnd)
		if err != nil {
			return errors.Wrap(err, "errors seeking to end of file")
		}
	}

	frame, err := log.framing.EncodeFrame(data)
	if err != nil {
		return err
	}

	_, err = log.f.Write(frame)
	if err != nil {
		return errors.Wrap(err, "error writng frame")
	}

	nextSeq, err := log.seq.Value()
	if err != nil {
		return errors.Wrap(err, "error reading current sequence number")
	}

	go log.seq.Set(nextSeq.(margaret.Seq) + 1)
	return nil
}
