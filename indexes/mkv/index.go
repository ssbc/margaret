// SPDX-License-Identifier: MIT

package mkv

import (
	"context"
	"encoding"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"

	"go.cryptoscope.co/luigi"
	"go.cryptoscope.co/margaret"
	"modernc.org/kv"

	librarian "go.cryptoscope.co/margaret/indexes"
)

func NewIndex(db *kv.DB, tipe interface{}) librarian.SeqSetterIndex {
	return &index{
		db:     db,
		tipe:   tipe,
		obvs:   make(map[librarian.Addr]luigi.Observable),
		curSeq: margaret.BaseSeq(-2),
	}
}

type index struct {
	l      sync.Mutex
	db     *kv.DB
	obvs   map[librarian.Addr]luigi.Observable
	tipe   interface{}
	curSeq margaret.Seq
}

func (idx *index) Flush() error { return nil }

func (idx *index) Close() error { return idx.db.Close() }

func (idx *index) Get(ctx context.Context, addr librarian.Addr) (luigi.Observable, error) {
	idx.l.Lock()
	defer idx.l.Unlock()

	obv, ok := idx.obvs[addr]
	if ok {
		return obv, nil
	}

	t := reflect.TypeOf(idx.tipe)
	v := reflect.New(t).Interface()

	data, err := idx.db.Get(nil, []byte(addr))
	if data == nil {
		obv := librarian.NewObservable(librarian.UnsetValue{addr}, idx.deleter(addr))
		idx.obvs[addr] = obv
		return roObv{obv}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("error loading data from store:%w", err)
	}

	if um, ok := v.(encoding.BinaryUnmarshaler); ok {
		if t.Kind() != reflect.Ptr {
			v = reflect.ValueOf(v).Elem().Interface()
		}

		err = um.UnmarshalBinary(data)
		return nil, fmt.Errorf("error unmarshaling using custom marshaler:%w", err)
	}

	err = json.Unmarshal(data, v)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling using json marshaler:%w", err)
	}

	if t.Kind() != reflect.Ptr {
		v = reflect.ValueOf(v).Elem().Interface()
	}

	obv = librarian.NewObservable(v, idx.deleter(addr))
	idx.obvs[addr] = obv
	return roObv{obv}, nil
}

func (idx *index) deleter(addr librarian.Addr) func() {
	return func() {
		delete(idx.obvs, addr)
	}
}

func (idx *index) Set(ctx context.Context, addr librarian.Addr, v interface{}) error {
	var (
		raw []byte
		err error
	)

	if m, ok := v.(encoding.BinaryMarshaler); ok {
		raw, err = m.MarshalBinary()
		if err != nil {
			return fmt.Errorf("error marshaling value using custom marshaler:%w", err)
		}
	} else {
		raw, err = json.Marshal(v)
		if err != nil {
			return fmt.Errorf("error marshaling value using json marshaler:%w", err)
		}
	}

	err = idx.db.Set([]byte(addr), raw)
	if err != nil {
		return fmt.Errorf("error in store:%w", err)
	}

	idx.l.Lock()
	defer idx.l.Unlock()

	obv, ok := idx.obvs[addr]
	if ok {
		err = obv.Set(v)
		if err != nil {
			return fmt.Errorf("error setting value in observable:%w", err)
		}
	}

	return nil
}

func (idx *index) Delete(ctx context.Context, addr librarian.Addr) error {
	err := idx.db.Delete([]byte(addr))
	if err != nil {
		return fmt.Errorf("error in store:%w", err)
	}

	idx.l.Lock()
	defer idx.l.Unlock()

	obv, ok := idx.obvs[addr]
	if ok {
		err = obv.Set(librarian.UnsetValue{addr})
		if err != nil {
			return fmt.Errorf("error setting value in observable:%w", err)
		}
	}

	return nil
}

func (idx *index) SetSeq(seq margaret.Seq) error {
	var (
		raw  = make([]byte, 8)
		err  error
		addr librarian.Addr = "__current_observable"
	)

	binary.BigEndian.PutUint64(raw, uint64(seq.Seq()))

	err = idx.db.Set([]byte(addr), raw)
	if err != nil {
		return fmt.Errorf("error during mkv update (%T): %w", seq.Seq(), err)
	}

	idx.l.Lock()
	defer idx.l.Unlock()

	idx.curSeq = seq

	return nil
}

func (idx *index) GetSeq() (margaret.Seq, error) {
	var addr = "__current_observable"

	idx.l.Lock()
	defer idx.l.Unlock()

	if idx.curSeq.Seq() != -2 {
		return idx.curSeq, nil
	}

	data, err := idx.db.Get(nil, []byte(addr))
	if err != nil {
		return margaret.BaseSeq(-2), fmt.Errorf("error getting item:%w", err)
	}
	if data == nil {
		return margaret.SeqEmpty, nil
	}

	if l := len(data); l != 8 {
		return margaret.BaseSeq(-2), fmt.Errorf("expected data of length 8, got %v", l)
	}

	idx.curSeq = margaret.BaseSeq(binary.BigEndian.Uint64(data))

	return idx.curSeq, nil
}

type roObv struct {
	luigi.Observable
}

func (obv roObv) Set(interface{}) error {
	return errors.New("read-only observable")
}
