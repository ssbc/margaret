package roaringfiles

import (
	"context"
	"strings"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/pkg/errors"
	"go.cryptoscope.co/luigi"
	"go.cryptoscope.co/margaret"
)

type query struct {
	l    sync.Mutex
	log  *sublog
	bmap *roaring.Bitmap

	nextSeq, lt margaret.BaseSeq

	limit   int
	live    bool
	seqWrap bool
}

func (qry *query) Gt(s margaret.Seq) error {
	if qry.nextSeq > margaret.SeqEmpty {
		return errors.Errorf("lower bound already set")
	}

	qry.nextSeq = margaret.BaseSeq(s.Seq() + 1)
	return nil
}

func (qry *query) Gte(s margaret.Seq) error {
	if qry.nextSeq > margaret.SeqEmpty {
		return errors.Errorf("lower bound already set")
	}

	qry.nextSeq = margaret.BaseSeq(s.Seq())
	return nil
}

func (qry *query) Lt(s margaret.Seq) error {
	if qry.lt != margaret.SeqEmpty {
		return errors.Errorf("upper bound already set")
	}

	qry.lt = margaret.BaseSeq(s.Seq())
	return nil
}

func (qry *query) Lte(s margaret.Seq) error {
	if qry.lt != margaret.SeqEmpty {
		return errors.Errorf("upper bound already set")
	}

	qry.lt = margaret.BaseSeq(s.Seq() + 1)
	return nil
}

func (qry *query) Limit(n int) error {
	qry.limit = n
	return nil
}

func (qry *query) Live(live bool) error {
	qry.live = live
	return nil
}

func (qry *query) SeqWrap(wrap bool) error {
	qry.seqWrap = wrap
	return nil
}

func (qry *query) Reverse(rev bool) error {
	if rev == false {
		return nil
	}
	return errors.Errorf("TODO:reverse")
}

func (qry *query) Next(ctx context.Context) (interface{}, error) {
	qry.l.Lock()
	defer qry.l.Unlock()

	if qry.limit == 0 {
		return nil, luigi.EOS{}
	}
	qry.limit--

	if qry.nextSeq == margaret.SeqEmpty {
		qry.nextSeq = 0
	}

	if qry.lt != margaret.SeqEmpty {
		if qry.nextSeq >= qry.lt {
			return nil, luigi.EOS{}
		}
	}

	var v interface{}
	seqVal, err := qry.bmap.Select(uint32(qry.nextSeq.Seq()))
	v = margaret.BaseSeq(seqVal)
	if err != nil {
		if !strings.Contains(err.Error(), "th integer in a bitmap with only ") {
			return nil, errors.Wrapf(err, "roaringfiles/qry: error in read transaction (%T)", err)
		}

		// key not found, so we reached the end
		// abort if not a live query, else wait until it's written
		if !qry.live {
			return nil, luigi.EOS{}
		}

		v, err = qry.livequery(ctx)
		if err != nil {
			return nil, err
		}
	}

	defer func() { qry.nextSeq++ }()

	if qry.seqWrap {
		return margaret.WrapWithSeq(v, qry.nextSeq), nil
	}

	return v, nil
}

func (qry *query) livequery(ctx context.Context) (interface{}, error) {
	wait := make(chan interface{})
	closed := make(chan struct{})

	// register waiter for new message
	var cancel func()
	cancel = qry.log.seq.Register(luigi.FuncSink(
		func(ctx context.Context, v interface{}, err error) error {
			if err != nil {
				close(closed)
				return nil
			}

			// TODO: maybe only accept == and throw error on >?
			if v.(margaret.Seq).Seq() >= qry.nextSeq.Seq() {
				wait <- v
			}

			return nil
		}))
	defer cancel()

	// release the lock so we don't block, but re-acquire it to increment nextSeq
	qry.l.Unlock()
	defer qry.l.Lock()

	var (
		v   interface{}
		err error
	)

	select {
	case w := <-wait:
		v = w
	case <-closed:
		err = errors.New("seq observable closed")
	case <-ctx.Done():
		err = errors.Wrap(ctx.Err(), "cancelled while waiting for value to be written")
	}

	return v, err
}
