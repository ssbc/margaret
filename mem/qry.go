// SPDX-License-Identifier: MIT

package mem // import "go.cryptoscope.co/margaret/mem"

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"go.cryptoscope.co/luigi"

	"go.cryptoscope.co/margaret"
)

type memlogQuery struct {
	log *memlog
	cur *memlogElem

	gt, lt, gte, lte int64

	limit   int
	live    bool
	seqWrap bool
	reverse bool
}

func (qry *memlogQuery) seek(ctx context.Context) error {
	var err error

	if qry.gt != margaret.SeqEmpty {
		if qry.cur.seq > qry.gt {
			qry.cur = qry.log.head
		}

		for (qry.cur.seq + 1) <= qry.gt {
			qry.cur, err = qry.cur.waitNext(ctx, &qry.log.l)
			if err != nil {
				return err
			}
		}
	} else if qry.gte != margaret.SeqEmpty {
		if qry.cur.seq > qry.gte {
			qry.cur = qry.log.head
		}

		for (qry.cur.seq + 1) < qry.gte {
			qry.cur, err = qry.cur.waitNext(ctx, &qry.log.l)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (qry *memlogQuery) Gt(s int64) error {
	if qry.gt != margaret.SeqEmpty || qry.gte != margaret.SeqEmpty {
		return fmt.Errorf("lower bound already set")
	}

	qry.gt = s
	return nil
}

func (qry *memlogQuery) Gte(s int64) error {
	if qry.gt != margaret.SeqEmpty || qry.gte != margaret.SeqEmpty {
		return fmt.Errorf("lower bound already set")
	}

	qry.gte = s
	return nil
}

func (qry *memlogQuery) Lt(s int64) error {
	if qry.lt != margaret.SeqEmpty || qry.lte != margaret.SeqEmpty {
		return fmt.Errorf("upper bound already set")
	}

	qry.lt = s
	return nil
}

func (qry *memlogQuery) Lte(s int64) error {
	if qry.lt != margaret.SeqEmpty || qry.lte != margaret.SeqEmpty {
		return fmt.Errorf("upper bound already set")
	}

	qry.lte = s
	return nil
}

func (qry *memlogQuery) Limit(n int) error {
	qry.limit = n
	return nil
}

func (qry *memlogQuery) Live(live bool) error {
	qry.live = live
	return nil
}

func (qry *memlogQuery) SeqWrap(wrap bool) error {
	qry.seqWrap = wrap
	return nil
}

func (qry *memlogQuery) Reverse(yes bool) error {
	qry.reverse = yes
	if yes {
		qry.cur = qry.log.tail
	}
	return nil
}

func (qry *memlogQuery) Next(ctx context.Context) (interface{}, error) {
	if qry.limit == 0 {
		return nil, luigi.EOS{}
	}
	qry.limit--

	qry.log.l.Lock()
	defer qry.log.l.Unlock()

	if qry.reverse {
		if qry.cur == qry.log.head {
			return qry.cur.v, luigi.EOS{}
		}
		v := qry.cur.v
		qry.cur = qry.cur.prev
		return v, nil
	}

	if qry.cur.seq <= qry.gt || qry.cur.seq < qry.gt {
		err := qry.seek(ctx)
		if err != nil {
			return nil, err
		}
	}

	// no new data yet and non-blocking
	if qry.cur.next == nil && !qry.live {
		return nil, luigi.EOS{}
	}

	if qry.lt != margaret.SeqEmpty && !(qry.cur.seq < (qry.lt)-1) {
		return nil, luigi.EOS{}
	} else if qry.lte != margaret.SeqEmpty && !(qry.cur.seq < qry.lte) {
		return nil, luigi.EOS{}
	}

	var err error
	qry.cur, err = qry.cur.waitNext(ctx, &qry.log.l)
	if err != nil {
		return nil, errors.Wrap(err, "error waiting for next value")
	}

	if qry.seqWrap {
		return margaret.WrapWithSeq(qry.cur.v, qry.cur.seq), nil
	}
	return qry.cur.v, nil
}
