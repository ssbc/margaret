package badger

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"

	"cryptoscope.co/go/luigi"
	"cryptoscope.co/go/margaret"
	"github.com/dgraph-io/badger"
	"github.com/pkg/errors"
)

type query struct {
	l sync.Mutex
	log *sublog

	nextSeq, lt margaret.Seq

	limit   int
	live    bool
	seqWrap bool
}

func (qry *query) Gt(s margaret.Seq) error {
	if qry.nextSeq > margaret.SeqEmpty {
		return errors.Errorf("lower bound already set")
	}

	qry.nextSeq = s + 1
	return nil
}

func (qry *query) Gte(s margaret.Seq) error {
	if qry.nextSeq > margaret.SeqEmpty {
		return errors.Errorf("lower bound already set")
	}

	qry.nextSeq = s
	return nil
}

func (qry *query) Lt(s margaret.Seq) error {
	if qry.lt != margaret.SeqEmpty {
		return errors.Errorf("upper bound already set")
	}

	qry.lt = s
	return nil
}

func (qry *query) Lte(s margaret.Seq) error {
	if qry.lt != margaret.SeqEmpty {
		return errors.Errorf("upper bound already set")
	}

	qry.lt = s + 1
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

func (qry *query) Next(ctx context.Context) (interface{}, error) {
	/*
	fmt.Println("qry.Next called", qry.log.prefix)
	defer debug.PrintStack()
	defer fmt.Println("qry.Nwxt returned", qry.log.prefix)
	*/

	qry.l.Lock()
	defer qry.l.Unlock()

	if qry.limit == 0 {
		return nil, luigi.EOS{}
	}
	qry.limit--

	if qry.nextSeq == margaret.SeqEmpty {
		qry.nextSeq = 0
	}

	// TODO: use iterator instead of getting sequentially

	nextSeqBs := make([]byte, 8)
	binary.BigEndian.PutUint64(nextSeqBs, uint64(qry.nextSeq))
	key := append(qry.log.prefix, nextSeqBs...)
	fmt.Println("getting key", key)

	var v interface{}

	err := qry.log.mlog.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return errors.Wrap(err, "error getting item")
		}

		data, err := item.Value()
		if err != nil {
			return errors.Wrap(err, "error getting value")
		}

		v, err = qry.log.mlog.codec.Unmarshal(data)
		return errors.Wrap(err, "error unmarshaling data")
	})
	if err != nil {
		// if key is not found, we haven't written that far yet
		if errors.Cause(err) == badger.ErrKeyNotFound {
			// abort if not a live query, else wait until it's written
			if !qry.live {
				fmt.Printf("error cause: %+v\n\n\n", err)
				return nil, luigi.EOS{}
			}

			wait := make(chan struct{})
			closed := make(chan struct{})

			var cancel func()
			cancel = qry.log.seq.Register(luigi.FuncSink(
				func(ctx context.Context, v interface{}, doClose bool) error {
					if doClose {
						close(closed)
						return nil
					}
					if v.(margaret.Seq) >= qry.nextSeq {
						close(wait)
					}

					return nil
				}))
			defer cancel()

			err := func() error {
				qry.l.Unlock()
				defer qry.l.Lock()

				select {
				case <-wait:
				case <-closed:
					return errors.New("seq observable closed")
				case <-ctx.Done():
					return errors.Wrap(ctx.Err(), "cancelled while waiting for value to be written")
				}
				return nil
			}()
			if err != nil {
				return nil, err
			}
		} else {
			return nil, errors.Wrap(err, "error in read transaction")
		}
	}

	defer func() { qry.nextSeq++ }()

	if qry.seqWrap {
		return margaret.WrapWithSeq(v, qry.nextSeq), nil
	}

	return v, nil
}
