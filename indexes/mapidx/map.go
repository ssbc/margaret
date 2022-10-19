// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

package mapidx // import "github.com/ssbc/margaret/indexes/mapidx"

import (
	"context"
	"fmt"
	"sync"

	"github.com/ssbc/go-luigi"
	"github.com/ssbc/margaret"
	"github.com/ssbc/margaret/indexes"
)

// New returns a new map based index
func New() indexes.SeqSetterIndex {
	return &mapSetterIndex{
		m:      make(map[indexes.Addr]luigi.Observable),
		curSeq: margaret.SeqEmpty,
	}
}

type mapSetterIndex struct {
	m      map[indexes.Addr]luigi.Observable
	curSeq int64
	l      sync.Mutex
}

func (idx *mapSetterIndex) Flush() error { return nil }
func (idx *mapSetterIndex) Close() error { return nil }

func (idx *mapSetterIndex) Get(_ context.Context, addr indexes.Addr) (luigi.Observable, error) {
	idx.l.Lock()
	defer idx.l.Unlock()

	obv, ok := idx.m[addr]
	if ok {
		return obv, nil
	}

	obv = luigi.NewObservable(indexes.UnsetValue{Addr: addr})
	idx.m[addr] = obv

	return obv, nil
}

func (idx *mapSetterIndex) Set(_ context.Context, addr indexes.Addr, v interface{}) error {
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

func (idx *mapSetterIndex) Delete(_ context.Context, addr indexes.Addr) error {
	idx.l.Lock()
	defer idx.l.Unlock()

	obv, ok := idx.m[addr]
	if ok {
		err := obv.Set(indexes.UnsetValue{Addr: addr})
		if err != nil {
			return fmt.Errorf("error setting observable: %w", err)
		}
	}

	return nil
}

func (idx *mapSetterIndex) GetSeq() (int64, error) {
	idx.l.Lock()
	defer idx.l.Unlock()

	return idx.curSeq, nil
}

func (idx *mapSetterIndex) SetSeq(seq int64) error {
	idx.l.Lock()
	defer idx.l.Unlock()

	idx.curSeq = seq

	return nil
}
