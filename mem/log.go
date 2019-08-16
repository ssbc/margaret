package mem // import "go.cryptoscope.co/margaret/mem"

import (
	"context"
	"io"
	"sync"

	"github.com/pkg/errors"
	"go.cryptoscope.co/luigi"
	"go.cryptoscope.co/margaret"
)

// TODO optimization idea: skip list
type memlogElem struct {
	v    interface{}
	seq  margaret.BaseSeq
	next *memlogElem
	prev *memlogElem

	wait chan struct{}
}

func (el *memlogElem) waitNext(ctx context.Context, m *sync.Mutex) (*memlogElem, error) {
	// closure to localize defer. We need to lock before accessing el.next in the return.
	err := func() error {
		// yes, first unlock, then lock. We need to release the mutex to
		// allow Appends to happen, but we need to lock again afterwards!
		m.Unlock()
		defer m.Lock()

		select {
		// wait until new element has been added
		case <-el.wait:
		// or context is canceled
		case <-ctx.Done():
			return ctx.Err()
		}

		return nil
	}()
	if err != nil {
		// return original element in error case
		return el, err
	}

	return el.next, nil
}

type memlog struct {
	l sync.Mutex

	seq        luigi.Observable
	head, tail *memlogElem

	closed bool
}

// New returns a new in-memory log
func New() margaret.Log {
	root := &memlogElem{
		seq:  margaret.SeqEmpty,
		wait: make(chan struct{}),
	}

	log := &memlog{
		seq:  luigi.NewObservable(margaret.SeqEmpty),
		head: root,
		tail: root,
	}

	return log
}

func (log *memlog) Close() error {
	log.l.Lock()
	defer log.l.Unlock()
	if log.closed {
		return io.ErrClosedPipe // already closed
	}
	log.closed = true
	return nil
}

func (log *memlog) Seq() luigi.Observable {
	return log.seq
}

func (log *memlog) Get(s margaret.Seq) (interface{}, error) {
	log.l.Lock()
	defer log.l.Unlock()
	if log.closed {
		return nil, io.ErrClosedPipe // already closed
	}

	var (
		cur = log.head
	)

	for cur.seq.Seq() < s.Seq() && cur.next != nil {
		cur = cur.next
	}

	if cur.seq.Seq() < s.Seq() {
		return nil, margaret.OOB
	}

	if cur.seq.Seq() > s.Seq() {
		// TODO maybe better handling of this case?
		panic("datastructure borked, sequence number missing")
	}

	return cur.v, nil
}

func (log *memlog) Query(specs ...margaret.QuerySpec) (luigi.Source, error) {
	log.l.Lock()
	defer log.l.Unlock()
	if log.closed {
		return nil, io.ErrClosedPipe // already closed
	}

	qry := &memlogQuery{
		log: log,
		cur: log.head,

		gt:  margaret.SeqEmpty,
		gte: margaret.SeqEmpty,
		lt:  margaret.SeqEmpty,
		lte: margaret.SeqEmpty,

		limit: -1, //i.e. no limit
	}

	for _, spec := range specs {
		err := spec(qry)
		if err != nil {
			return nil, err
		}
	}

	if qry.reverse && qry.live {
		return nil, errors.Errorf("memlog: can't do reverse and live")
	}

	return qry, nil
}

func (log *memlog) Append(v interface{}) (margaret.Seq, error) {
	log.l.Lock()
	defer log.l.Unlock()
	if log.closed {
		return nil, io.ErrClosedPipe // already closed
	}

	nxt := &memlogElem{
		v:    v,
		seq:  log.tail.seq + 1,
		wait: make(chan struct{}),
	}

	log.tail.next = nxt
	oldtail := log.tail
	nxt.prev = oldtail
	log.tail = log.tail.next

	close(oldtail.wait)
	log.seq.Set(log.tail.seq)

	return log.tail.seq, nil
}
