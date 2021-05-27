// SPDX-License-Identifier: MIT

package mapidx // import "go.cryptoscope.co/margaret/indexes/mapidx"

import (
	"context"
	"fmt"
	"sync"

	"go.cryptoscope.co/luigi"
	"go.cryptoscope.co/margaret"

	librarian "go.cryptoscope.co/margaret/indexes"
)

// New returns a new map based index
func New() librarian.SeqSetterIndex {
	return &mapSetterIndex{
		m:      make(map[librarian.Addr]luigi.Observable),
		curSeq: margaret.SeqEmpty,
	}
}

type mapSetterIndex struct {
	m      map[librarian.Addr]luigi.Observable
	curSeq margaret.Seq
	l      sync.Mutex
}

func (idx *mapSetterIndex) Flush() error { return nil }
func (idx *mapSetterIndex) Close() error { return nil }

func (idx *mapSetterIndex) Get(_ context.Context, addr librarian.Addr) (luigi.Observable, error) {
	idx.l.Lock()
	defer idx.l.Unlock()

	obv, ok := idx.m[addr]
	if ok {
		return obv, nil
	}

	obv = luigi.NewObservable(librarian.UnsetValue{Addr: addr})
	idx.m[addr] = obv

	return obv, nil
}

func (idx *mapSetterIndex) Set(_ context.Context, addr librarian.Addr, v interface{}) error {
	idx.l.Lock()
	defer idx.l.Unlock()

	obv, ok := idx.m[addr]
	if ok {
		err := obv.Set(v)
		if err != nil {
			return fmt.Errorf("error setting observable: %w", err)
		}
		return nil
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
		err := obv.Set(librarian.UnsetValue{Addr: addr})
		if err != nil {
			return fmt.Errorf("error setting observable: %w", err)
		}
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
