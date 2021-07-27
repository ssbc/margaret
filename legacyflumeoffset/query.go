package legacyflumeoffset

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"go.cryptoscope.co/luigi"
	"go.cryptoscope.co/margaret"
)

type lfoQuery struct {
	l     sync.Mutex
	log   *Log
	codec margaret.Codec

	nextOfst, lt int64

	limit   int
	live    bool
	seqWrap bool
	reverse bool
	close   chan struct{}
	err     error
}

func (qry *lfoQuery) Gt(s int64) error {
	return fmt.Errorf("TODO: implement gt")
	if qry.nextOfst > margaret.SeqEmpty {
		return fmt.Errorf("lower bound already set")
	}

	// TODO: seek to the next entry correctly
	qry.nextOfst = s + 1
	return nil
}

func (qry *lfoQuery) Gte(s int64) error {
	return fmt.Errorf("TODO: implement gte")
	if qry.nextOfst > margaret.SeqEmpty {
		return fmt.Errorf("lower bound already set")
	}

	qry.nextOfst = s
	return nil
}

func (qry *lfoQuery) Lt(s int64) error {
	return fmt.Errorf("TODO: implement lt")
	if qry.lt != margaret.SeqEmpty {
		return fmt.Errorf("upper bound already set")
	}

	qry.lt = s
	return nil
}

func (qry *lfoQuery) Lte(s int64) error {
	return fmt.Errorf("TODO: implement lte")
	if qry.lt != margaret.SeqEmpty {
		return fmt.Errorf("upper bound already set")
	}

	// TODO: seek to the previous entry correctly
	qry.lt = s + 1
	return nil
}

func (qry *lfoQuery) Limit(n int) error {
	qry.limit = n
	return nil
}

func (qry *lfoQuery) Live(live bool) error {
	return fmt.Errorf("live not supported")
	qry.live = live
	return nil
}

func (qry *lfoQuery) SeqWrap(wrap bool) error {
	qry.seqWrap = wrap
	return nil
}

func (qry *lfoQuery) Reverse(yes bool) error {
	return fmt.Errorf("TODO: implement reverse iteration")
	// qry.reverse = yes
	// if yes {
	// 	if err := qry.setCursorToLast(); err != nil {
	// 		return err
	// 	}
	// }
	return nil
}

func (qry *lfoQuery) Next(ctx context.Context) (interface{}, error) {
	qry.l.Lock()
	defer qry.l.Unlock()

	if qry.limit == 0 {
		return nil, luigi.EOS{}
	}
	qry.limit--

	if qry.nextOfst == margaret.SeqEmpty {
		if qry.reverse {
			return nil, luigi.EOS{}
		}
		qry.nextOfst = 0
	}

	qry.log.mu.Lock()
	defer qry.log.mu.Unlock()

	if qry.lt != margaret.SeqEmpty && !(qry.nextOfst < qry.lt) {
		return nil, luigi.EOS{}
	}

	v, sz, err := qry.log.readOffset(qry.nextOfst)
	if errors.Is(err, io.EOF) {
		if qry.live {
			return nil, fmt.Errorf("live not supported")
		}
		return v, luigi.EOS{}
	} else if errors.Is(err, margaret.ErrNulled) {
		// TODO: qry.skipNulled
		qry.nextOfst = qry.nextOfst + int64(sz)
		return margaret.ErrNulled, nil
	} else if err != nil {
		return nil, err
	}

	defer func() {
		if qry.reverse {
			qry.nextOfst = qry.nextOfst - int64(sz)
		} else {
			qry.nextOfst = qry.nextOfst + int64(sz)
		}
	}()

	if qry.seqWrap {
		return margaret.WrapWithSeq(v, qry.nextOfst), nil
	}

	return v, nil
}
