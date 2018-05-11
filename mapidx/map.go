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

func (idx *mapSetterIndex) GetSeq() (margaret.Seq, error) {
	idx.l.Lock()
	defer idx.l.Unlock()

	var addr librarian.Addr = "__current_observable"

	obv, ok := idx.m[addr]
	if !ok {
		return 0, nil
	}

	v, err := obv.Value()
	if err != nil {
		return 0, errors.Wrap(err, "error obtaining value from observable")
	}

	seq, ok := v.(margaret.Seq)
	if !ok {
		return 0, errors.Errorf("type error: expected %T, got %T", seq, v)
	}

	return seq, nil
}

func (idx *mapSetterIndex) SetSeq(seq margaret.Seq) error {
	idx.l.Lock()
	defer idx.l.Unlock()

	var addr librarian.Addr = "__current_observable"

	obv, ok := idx.m[addr]
	if ok {
		v, err := obv.Value()
		if err != nil {
			return errors.Wrap(err, "error getting old sequence number")
		}
		oldseq, ok := v.(margaret.Seq)
		if !ok {
			return errors.Errorf("type error: expected %T, got %T", oldseq, v)
		}
		if oldseq > seq {
			return errors.New("sequnce number not larger than last")
		}

		err = obv.Set(seq)
		return errors.Wrap(err, "error setting observable")
	}

	obv = luigi.NewObservable(seq)
	idx.m[addr] = obv

	return nil
}
