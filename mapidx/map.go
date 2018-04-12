package mapidx // import "cryptoscope.co/go/librarian/mapidx"

import (
	"context"
	"sync"

	"cryptoscope.co/go/luigi"
	"github.com/pkg/errors"

	"cryptoscope.co/go/librarian"
)

func New() librarian.SetterIndex {
	return &mapSetterIndex{
		m: make(map[librarian.Addr]luigi.Observable),
	}
}

type mapSetterIndex struct {
	m map[librarian.Addr]luigi.Observable
	l sync.Mutex
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
