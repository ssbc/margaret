// SPDX-License-Identifier: MIT

package badger // import "go.cryptoscope.co/librarian/badger"

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"log"
	"reflect"
	"sync"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/pkg/errors"
	"go.cryptoscope.co/luigi"
	"go.cryptoscope.co/margaret"

	"go.cryptoscope.co/librarian"
)

type setOp struct {
	addr []byte
	val  []byte
}

type index struct {
	stop    context.CancelFunc
	running context.Context

	l *sync.Mutex

	tickPersistAll, tickIfFull *time.Ticker

	batchLimit uint
	batches    []setOp

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

		tickPersistAll: time.NewTicker(10 * time.Second),
		tickIfFull:     time.NewTicker(3 * time.Second),

		batchLimit: 4096,
		batches:    make([]setOp, 0),

		db:     db,
		tipe:   tipe,
		obvs:   make(map[librarian.Addr]luigi.Observable),
		curSeq: margaret.BaseSeq(-2),
	}
	go idx.writeBatches()
	return idx
}

func (idx *index) Close() error {
	idx.stop()
	idx.tickIfFull.Stop()
	idx.tickPersistAll.Stop()
	log.Println("closing!! DIRTY HACK so that writeBatches returns")
	idx.flushBatches()
	return nil
}

func (idx *index) flushBatches() {
	start := time.Now()
	var raw = make([]byte, 8)
	err := idx.db.Update(func(txn *badger.Txn) error {
		useq := uint64(idx.curSeq)
		binary.BigEndian.PutUint64(raw, useq)

		err := txn.Set([]byte("__current_observable"), raw)
		if err != nil {
			return errors.Wrap(err, "error setting seq")
		}

		for bi, op := range idx.batches {
			err := txn.Set(op.addr, op.val)
			if err != nil {
				return errors.Wrapf(err, "error setting batch #%d", bi)
			}
		}
		return nil
	})
	if err != nil {
		log.Println(errors.Wrapf(err, "error in badger transaction (update) %d", len(idx.batches)))
		return
	}
	log.Println("curr seq:", idx.curSeq.Seq(), "writing batch:", len(idx.batches), "took", time.Since(start))

}

func (idx *index) writeBatches() {

	for {
		var writeAll = false
		select {
		case <-idx.tickPersistAll.C:
			writeAll = true
		default:
		}

		select {
		case <-idx.tickIfFull.C:

		case <-idx.running.Done():
			log.Println("index done", idx.running.Err())
			return
		}
		idx.l.Lock()
		n := uint(len(idx.batches))

		if !writeAll {
			if n < idx.batchLimit {
				if n > 0 {
					log.Println("batch too small:", n)
				}
				idx.l.Unlock()
				continue
			}
		}
		if n == 0 {
			idx.l.Unlock()
			continue
		}

		idx.flushBatches()
		idx.batches = []setOp{}
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

	t := reflect.TypeOf(idx.tipe)
	v := reflect.New(t).Interface()

	err := idx.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(addr))
		if err != nil {
			return errors.Wrap(err, "error getting item")
		}

		err = item.Value(func(data []byte) error {
			if um, ok := v.(librarian.Unmarshaler); ok {
				if t.Kind() != reflect.Ptr {
					v = reflect.ValueOf(v).Elem().Interface()
				}

				err = um.Unmarshal(data)
				return errors.Wrap(err, "error unmarshaling using custom marshaler")
			}

			err = json.Unmarshal(data, v)
			if err != nil {
				return errors.Wrap(err, "error unmarshaling using json marshaler")
			}

			if t.Kind() != reflect.Ptr {
				v = reflect.ValueOf(v).Elem().Interface()
			}
			return nil
		})
		if err != nil {
			return errors.Wrap(err, "error getting value")
		}

		return err
	})

	if err != nil && errors.Cause(err) != badger.ErrKeyNotFound {
		return nil, errors.Wrap(err, "error in badger transaction (view)")
	}

	if errors.Cause(err) == badger.ErrKeyNotFound {
		obv = librarian.NewObservable(librarian.UnsetValue{addr}, idx.deleter(addr))
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

	if m, ok := v.(librarian.Marshaler); ok {
		raw, err = m.Marshal()
		if err != nil {
			return errors.Wrap(err, "error marshaling value using custom marshaler")
		}
	} else {
		raw, err = json.Marshal(v)
		if err != nil {
			return errors.Wrap(err, "error marshaling value using json marshaler")
		}
	}

	idx.l.Lock()
	defer idx.l.Unlock()
	idx.batches = append(idx.batches, setOp{addr: []byte(addr), val: raw})

	obv, ok := idx.obvs[addr]
	if ok {
		err = obv.Set(v)
		err = errors.Wrap(err, "error setting value in observable")
	}

	return err
}

func (idx *index) Delete(ctx context.Context, addr librarian.Addr) error {
	err := idx.db.Update(func(txn *badger.Txn) error {
		err := txn.Delete([]byte(addr))
		return errors.Wrap(err, "error deleting item")
	})
	if err != nil {
		return errors.Wrap(err, "error in badger transaction (update)")
	}

	idx.l.Lock()
	defer idx.l.Unlock()

	obv, ok := idx.obvs[addr]
	if ok {
		err = obv.Set(librarian.UnsetValue{Addr: addr})
		err = errors.Wrap(err, "error setting value in observable")
	}

	return err
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
			return errors.Wrap(err, "error getting item")
		}

		err = item.Value(func(data []byte) error {

			if l := len(data); l != 8 {
				return errors.Errorf("expected data of length 8, got %v", l)
			}

			idx.curSeq = margaret.BaseSeq(binary.BigEndian.Uint64(data))

			return nil
		})
		if err != nil {
			return errors.Wrap(err, "error getting value")
		}

		return nil
	})

	if err != nil {
		if errors.Cause(err) == badger.ErrKeyNotFound {
			return margaret.SeqEmpty, nil
		}
		return margaret.BaseSeq(0), errors.Wrap(err, "error in badger transaction (view)")
	}

	return idx.curSeq, nil
}

type roObv struct {
	luigi.Observable
}

func (obv roObv) Set(interface{}) error {
	return errors.New("read-only observable")
}
