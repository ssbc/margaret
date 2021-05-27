// SPDX-License-Identifier: MIT

package badger // import "go.cryptoscope.co/margaret/indexes/badger"

import (
	"context"
	"encoding"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v3"
	"go.cryptoscope.co/luigi"
	"go.cryptoscope.co/margaret"

	librarian "go.cryptoscope.co/margaret/indexes"
)

// badger starts to complain >100k
var batchFullLimit uint32 = 75000

// NASTY TESTING HACK
func init() {
	limit, has := os.LookupEnv("LIBRARIAN_WRITEALL")
	if has {
		parsed, err := strconv.ParseUint(limit, 10, 32)
		if err != nil {
			panic(err)
		}
		log.Println("[librarian/badger] overwrote batch limit", parsed)
		batchFullLimit = uint32(parsed)
	}
}

type setOp struct {
	addr []byte
	val  []byte
}

type index struct {
	stop    context.CancelFunc
	running context.Context

	l *sync.Mutex

	// these control periodic persistence
	tickPersistAll, tickIfFull *time.Ticker

	batchLowerLimit uint   // only write if there are more batches then this
	batchFullLimit  uint32 // more than this cause an problem in badger

	nextbatch []setOp

	db *badger.DB

	obvs   map[librarian.Addr]luigi.Observable
	tipe   interface{}
	curSeq margaret.BaseSeq
}

func NewIndex(db *badger.DB, tipe interface{}) librarian.SeqSetterIndex {
	ctx, cancel := context.WithCancel(context.TODO())
	idx := &index{
		stop:    cancel,
		running: ctx,

		l: &sync.Mutex{},

		tickPersistAll: time.NewTicker(17 * time.Second),
		tickIfFull:     time.NewTicker(5 * time.Second),

		batchLowerLimit: 32000,
		batchFullLimit:  batchFullLimit,
		nextbatch:       make([]setOp, 0),

		db:     db,
		tipe:   tipe,
		obvs:   make(map[librarian.Addr]luigi.Observable),
		curSeq: margaret.BaseSeq(-2),
	}
	go idx.writeBatches()
	return idx
}

func (idx *index) Flush() error {
	idx.l.Lock()
	defer idx.l.Unlock()

	if err := idx.flushBatch(); err != nil {
		return err
	}
	return nil
}

func (idx *index) Close() error {
	idx.l.Lock()
	defer idx.l.Unlock()

	idx.stop()
	idx.tickIfFull.Stop()
	idx.tickPersistAll.Stop()

	err := idx.flushBatch()
	if err != nil {
		return fmt.Errorf("librarian/badger: failed to flush remaining batched operations: %w", err)
	}

	if err := idx.db.Close(); err != nil {
		return fmt.Errorf("librarian/badger: failed to close backing store: %w", err)
	}

	return nil
}

func (idx *index) flushBatch() error {
	var raw = make([]byte, 8)
	err := idx.db.Update(func(txn *badger.Txn) error {
		useq := uint64(idx.curSeq)
		binary.BigEndian.PutUint64(raw, useq)

		err := txn.Set([]byte("__current_observable"), raw)
		if err != nil {
			return fmt.Errorf("error setting seq: %w", err)
		}

		for bi, op := range idx.nextbatch {
			err := txn.Set(op.addr, op.val)
			if err != nil {
				return fmt.Errorf("error setting batch #%d: %w", bi, err)
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("error in badger transaction (update) %d: %w", len(idx.nextbatch), err)

	}
	idx.nextbatch = []setOp{}
	return nil
}

func (idx *index) writeBatches() {

	for {
		var writeAll = false

		// if this was in the same select with the ticker below,
		// the ticker with the smaller durration would always overrule the longer one
		select {
		case <-idx.tickPersistAll.C:
			writeAll = true
		default:
		}

		select {
		case <-idx.tickIfFull.C:

		case <-idx.running.Done():
			return
		}
		idx.l.Lock()
		n := uint(len(idx.nextbatch))

		if !writeAll {
			if n < idx.batchLowerLimit {
				idx.l.Unlock()
				continue
			}
		}
		if n == 0 {
			idx.l.Unlock()
			continue
		}

		err := idx.flushBatch()
		if err != nil {
			// TODO: maybe set error and stop further writes?
			log.Println("librarian: flushing failed", err)
		}
		idx.l.Unlock()
	}
}

func (idx *index) Get(ctx context.Context, addr librarian.Addr) (luigi.Observable, error) {
	idx.l.Lock()
	defer idx.l.Unlock()

	obv, ok := idx.obvs[addr]
	if ok {
		return obv, nil
	}

	if err := idx.flushBatch(); err != nil {
		return nil, err
	}

	t := reflect.TypeOf(idx.tipe)
	v := reflect.New(t).Interface()

	err := idx.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(addr))
		if err != nil {
			return fmt.Errorf("error getting item: %w", err)
		}

		err = item.Value(func(data []byte) error {
			if um, ok := v.(encoding.BinaryUnmarshaler); ok {
				if t.Kind() != reflect.Ptr {
					v = reflect.ValueOf(v).Elem().Interface()
				}

				err = um.UnmarshalBinary(data)
				return fmt.Errorf("error unmarshaling using custom marshaler: %w", err)
			}

			err = json.Unmarshal(data, v)
			if err != nil {
				return fmt.Errorf("error unmarshaling using json marshaler: %w", err)
			}

			if t.Kind() != reflect.Ptr {
				v = reflect.ValueOf(v).Elem().Interface()
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("error getting value: %w", err)
		}

		return err
	})

	if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
		return nil, fmt.Errorf("error in badger transaction (view): %w", err)
	}

	if errors.Is(err, badger.ErrKeyNotFound) {
		obv = librarian.NewObservable(librarian.UnsetValue{Addr: addr}, idx.deleter(addr))
	} else {
		obv = librarian.NewObservable(v, idx.deleter(addr))
	}

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
			return fmt.Errorf("error marshaling value using custom marshaler: %w", err)
		}
	} else {
		raw, err = json.Marshal(v)
		if err != nil {
			return fmt.Errorf("error marshaling value using json marshaler: %w", err)
		}
	}

	idx.l.Lock()
	defer idx.l.Unlock()
	batchedOp := setOp{
		addr: []byte(addr),
		val:  raw,
	}
	idx.nextbatch = append(idx.nextbatch, batchedOp)

	if n := uint32(len(idx.nextbatch)); n > idx.batchFullLimit {
		err = idx.flushBatch()
		if err != nil {
			return fmt.Errorf("failed to write big batch (%d): %w", n, err)
		}
	}

	obv, ok := idx.obvs[addr]
	if ok {
		err = obv.Set(v)
		if err != nil {
			return fmt.Errorf("error setting value in observable: %w", err)
		}
	}

	return nil
}

func (idx *index) Delete(ctx context.Context, addr librarian.Addr) error {
	err := idx.db.Update(func(txn *badger.Txn) error {
		err := txn.Delete([]byte(addr))
		if err != nil {
			return fmt.Errorf("error deleting item: %w", err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("error in badger transaction (update): %w", err)
	}

	idx.l.Lock()
	defer idx.l.Unlock()

	obv, ok := idx.obvs[addr]
	if ok {
		err = obv.Set(librarian.UnsetValue{Addr: addr})
		if err != nil {
			return fmt.Errorf("error setting value in observable: %w", err)
		}
	}

	return nil
}

func (idx *index) SetSeq(seq margaret.Seq) error {
	idx.l.Lock()
	defer idx.l.Unlock()

	idx.curSeq = margaret.BaseSeq(seq.Seq())
	return nil
}

func (idx *index) GetSeq() (margaret.Seq, error) {
	var addr = "__current_observable"

	idx.l.Lock()
	defer idx.l.Unlock()

	if idx.curSeq.Seq() != -2 {
		return idx.curSeq, nil
	}

	err := idx.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(addr))
		if err != nil {
			return fmt.Errorf("error getting item: %w", err)
		}

		err = item.Value(func(data []byte) error {

			if l := len(data); l != 8 {
				return fmt.Errorf("expected data of length 8, got %v", l)
			}

			idx.curSeq = margaret.BaseSeq(binary.BigEndian.Uint64(data))

			return nil
		})
		if err != nil {
			return fmt.Errorf("error getting value: %w", err)
		}

		return nil
	})

	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return margaret.SeqEmpty, nil
		}
		return margaret.BaseSeq(0), fmt.Errorf("error in badger transaction (view): %w", err)
	}

	return idx.curSeq, nil
}

type roObv struct {
	luigi.Observable
}

func (obv roObv) Set(interface{}) error {
	return errors.New("read-only observable")
}
