package mapidx // import "cryptoscope.co/go/librarian/mapidx"

import (
	"context"
	"sync"

	"cryptoscope.co/go/luigi"
	"cryptoscope.co/go/margaret"
	"github.com/pkg/errors"

	"cryptoscope.co/go/librarian"
)

func New() librarian.SeqSetterIndex {
	return &mapSetterIndex{
		m:      make(map[librarian.Addr]luigi.Observable),
		curSeq: -1,
	}
}

type mapSetterIndex struct {
	m      map[librarian.Addr]luigi.Observable
	curSeq margaret.Seq
	l      sync.Mutex
}

func (idx *mapSetterIndex) Get(_ context.Context, addr librarian.Addr) (luigi.Observable, error) {
	idx.l.Lock()
	defer idx.l.Unlock()

	obv, ok := idx.m[addr]
	if ok {
		return obv, nil
	}

	obv = luigi.NewObservable(librarian.UnsetValue{addr})
	idx.m[addr] = obv

	return obv, nil
}

func (idx *mapSetterIndex) Set(_ context.Context, addr librarian.Addr, v interface{}) error {
	idx.l.Lock()
	defer idx.l.Unlock()

	obv, ok := idx.m[addr]
	if ok {
		err := obv.Set(v)
		return errors.Wrap(err, "error setting observable")
	}

	obv = luigi.NewObservable(v)
	idx.m[addr] = obv

	return nil
}

func (idx *mapSetterIndex) Delete(_ context.Context, addr librarian.Addr) error {
	idx.l.Lock()
	defer idx.l.Unlock()

	obv, ok := idx.m[addr]
	if ok {
		err := obv.Set(librarian.UnsetValue{addr})
		return errors.Wrap(err, "error setting observable")
	}

	return nil
}

func (idx *mapSetterIndex) GetSeq() (margaret.Seq, error) {
	idx.l.Lock()
	defer idx.l.Unlock()

	return idx.curSeq, nil
}

func (idx *mapSetterIndex) SetSeq(seq margaret.Seq) error {
	idx.l.Lock()
	defer idx.l.Unlock()

	idx.curSeq = seq

	return nil
}
