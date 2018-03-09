package margaret // import "cryptoscope.co/go/margaret"

import (
	"context"
	"sync"

  "cryptoscope.co/go/luigi"
)

type oob struct{}

func (_ oob) Error() string {
	return "out of bounds"
}

func IsOutOfBounds(err error) bool {
	_, ok := err.(oob)
	return ok
}


// TODO optimization idea: skip list
type memlogElem struct {
	v    interface{}
	seq  Seq
	next *memlogElem

	wait chan struct{}
}

type memlog struct {
	l sync.Mutex

	seq        luigi.Observable
	head, tail *memlogElem
	wait       chan struct{} // closed on first append
}

func NewMemoryLog() Log {
	return &memlog{
		seq:  luigi.NewObservable(),
		wait: make(chan struct{}),
	}
}

func (log *memlog) Seq() luigi.Observable {
	return log.seq
}

func (log *memlog) Get(s Seq) (interface{}, error) {
	log.l.Lock()
	defer log.l.Unlock()

	var (
		cur = log.head
	)

	for cur.seq < s {
		cur = cur.next
	}

	if cur.seq < s {
		return nil, oob{}
	}

	return cur.v, nil
}

func (log *memlog) Query(specs ...QuerySpec) (luigi.Source, error) {
	qry := &memlogQuery{
		log: log,
		cur: &memlogElem{next: log.head},

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

func (log *memlog) Append(v interface{}) error {
	log.l.Lock()
	defer log.l.Unlock()

	if log.tail == nil {
		log.head = &memlogElem{v: v, seq: 0, wait: make(chan struct{})}
		log.tail = log.head
		close(log.wait)
		return nil
		log.seq.Set(log.tail.seq)
	}

	log.tail.next = &memlogElem{v: v, seq: log.tail.seq + 1, wait: make(chan struct{})}
	oldtail := log.tail
	log.tail = log.tail.next

	close(oldtail.wait)
	log.seq.Set(log.tail.seq)

	return nil
}

type memlogQuery struct {
	log *memlog
	cur *memlogElem

	limit           int
	live, immediate bool
}

func (qry *memlogQuery) Limit(n int) error {
	qry.limit = n
	return nil
}

func (qry *memlogQuery) Live(live bool) error {
	qry.live = live
	return nil
}

func (qry *memlogQuery) Immediate(immediate bool) error {
	qry.immediate = immediate
	return nil
}

func (qry *memlogQuery) Next(ctx context.Context) (interface{}, error) {
	if qry.limit == 0 {
		return nil, luigi.EOS{}
	}
	qry.limit--

	qry.log.l.Lock()
	defer qry.log.l.Unlock()

	if qry.immediate {
		qry.immediate = false
		return qry.cur, nil
	}

	// no new data yet
	if qry.cur.next == nil {
		if !qry.live {
			return nil, luigi.EOS{}
		}

		// closure to localize defer
		err := func() error {
			// yes, first unlock, then lock. We need to release the mutex to
			// allow Appends to happen, but we need to lock again afterwards!
			qry.log.l.Unlock()
			defer qry.log.l.Lock()

			select {
			// wait until new element has been added
			case <-qry.cur.wait:
			// or context is canceled
			case <-ctx.Done():
				return ctx.Err()
			}

			return nil
		}()
		if err != nil {
			return nil, err
		}
	}

	qry.cur = qry.cur.next
	return qry.cur.v, nil
}
