// SPDX-License-Identifier: MIT

package roaringfiles

import (
	"context"
	"fmt"
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

	if qry.limit == 0 {
		qry.l.Unlock()
		return nil, luigi.EOS{}
	}
	qry.limit--

	if qry.nextSeq == margaret.SeqEmpty {
		qry.nextSeq = 0
	}

	if qry.lt != margaret.SeqEmpty {
		if qry.nextSeq >= qry.lt {
			qry.l.Unlock()
			return nil, luigi.EOS{}
		}
	}

	var v interface{}
	seqVal, err := qry.bmap.Select(uint32(qry.nextSeq.Seq()))
	v = margaret.BaseSeq(seqVal)
	if err != nil {
		if !strings.Contains(err.Error(), "th integer in a bitmap with only ") {
			qry.l.Unlock()
			return nil, errors.Wrapf(err, "roaringfiles/qry: error in read transaction (%T)", err)
		}

		// key not found, so we reached the end
		// abort if not a live query, else wait until it's written
		if !qry.live {
			qry.l.Unlock()
			return nil, luigi.EOS{}
		}

		return qry.livequery(ctx)
	}

	if qry.seqWrap {
		v = margaret.WrapWithSeq(v, qry.nextSeq)
		qry.nextSeq++
		qry.l.Unlock()
		return v, nil
	}

	qry.nextSeq++
	qry.l.Unlock()
	return v, nil
}

func (qry *query) livequery(ctx context.Context) (interface{}, error) {
	wait := make(chan margaret.Seq)
	closed := make(chan struct{})

	currNextSeq := qry.nextSeq.Seq()

	// register waiter for new message
	cancel := qry.log.seq.Register(luigi.FuncSink(
		func(ctx context.Context, v interface{}, err error) error {
			fmt.Println("live sub query boradcast triggered", currNextSeq, v, err)
			if err != nil {
				close(closed)
				return nil
			}

			seqV, ok := v.(margaret.Seq)
			if !ok {
				return errors.Errorf("lievquery: expected sequence value from observable")
			}

			if seqV.Seq() == currNextSeq {
				wait <- seqV
			}

			return nil
		}))
	qry.l.Unlock()

	var (
		v   interface{}
		err error
	)

	select {
	case seqV := <-wait:
		v, err = qry.log.get(seqV)
		if !qry.seqWrap { // simpler to have two +1's here then a defer
			qry.nextSeq++
		}
	case <-closed:
		err = errors.New("seq observable closed")
	case <-ctx.Done():
		err = errors.Wrap(ctx.Err(), "cancelled while waiting for value to be written")
	}

	cancel()

	if err != nil {
		return nil, errors.Wrap(err, "livequery failed to retreive value")
	}

	if qry.seqWrap {
		v = margaret.WrapWithSeq(v, qry.nextSeq)
		qry.nextSeq++
		return v, nil
	}

	return v, err
}
