package legacyflumeoffset

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/pkg/errors"
	"go.cryptoscope.co/luigi"
	"go.cryptoscope.co/margaret"
)

type lfoQuery struct {
	l     sync.Mutex
	log   *Log
	codec margaret.Codec

	nextOfst, lt margaret.BaseSeq

	limit   int
	live    bool
	seqWrap bool
	reverse bool
	close   chan struct{}
	err     error
}

func (qry *lfoQuery) Gt(s margaret.Seq) error {
	if qry.nextOfst > margaret.SeqEmpty {
		return errors.Errorf("lower bound already set")
	}

	qry.nextOfst = margaret.BaseSeq(s.Seq() + 1)
	return nil
}

func (qry *lfoQuery) Gte(s margaret.Seq) error {
	if qry.nextOfst > margaret.SeqEmpty {
		return errors.Errorf("lower bound already set")
	}

	qry.nextOfst = margaret.BaseSeq(s.Seq())
	return nil
}

func (qry *lfoQuery) Lt(s margaret.Seq) error {
	if qry.lt != margaret.SeqEmpty {
		return errors.Errorf("upper bound already set")
	}

	qry.lt = margaret.BaseSeq(s.Seq())
	return nil
}

func (qry *lfoQuery) Lte(s margaret.Seq) error {
	if qry.lt != margaret.SeqEmpty {
		return errors.Errorf("upper bound already set")
	}

	qry.lt = margaret.BaseSeq(s.Seq() + 1)
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

// func (qry *lfoQuery) setCursorToLast() error {
// 	v, err := qry.log.seq.Value()
// 	if err != nil {
// 		return errors.Wrap(err, "setCursorToLast: failed to establish current value")
// 	}
// 	currSeq, ok := v.(margaret.Seq)
// 	if !ok {
// 		return errors.Errorf("setCursorToLast: failed to establish current value")
// 	}
// 	qry.nextOfst = margaret.BaseSeq(currSeq.Seq())
// 	return nil
// }

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
	if errors.Cause(err) == io.EOF {
		if qry.live {
			return nil, fmt.Errorf("live not supported")
		}
		return v, luigi.EOS{}
	} else if errors.Cause(err) == margaret.ErrNulled {
		// TODO: qry.skipNulled
		qry.nextOfst = margaret.BaseSeq(qry.nextOfst.Seq() + int64(sz))
		return margaret.ErrNulled, nil
	} else if err != nil {
		return nil, err
	}

	// we waited until the value is in the log - now read it again

	// v, err = qry.log.readOffset(qry.nextOfst)
	// if errors.Cause(err) == io.EOF {
	// 	return nil, io.ErrUnexpectedEOF
	// } else if err != nil {
	// 	return nil, err
	// }

	defer func() {
		if qry.reverse {
			qry.nextOfst = margaret.BaseSeq(qry.nextOfst.Seq() - int64(sz))
		} else {
			qry.nextOfst = margaret.BaseSeq(qry.nextOfst.Seq() + int64(sz))
		}
	}()

	if qry.seqWrap {
		return margaret.WrapWithSeq(v, margaret.BaseSeq(qry.nextOfst)), nil
	}

	return v, nil
}
