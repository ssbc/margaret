package badger // import "cryptoscope.co/go/librarian/badger"

import (
	"context"
	"encoding/json"
	"reflect"
	"sync"

	"cryptoscope.co/go/luigi"
	"github.com/dgraph-io/badger"
	"github.com/pkg/errors"

	"cryptoscope.co/go/librarian"
)

func NewIndex(db *badger.DB, tipe interface{}) librarian.SetterIndex {
	return &index{
		db:   db,
		tipe: tipe,
		obvs: make(map[librarian.Addr]luigi.Observable),
	}
}

type index struct {
	l    sync.Mutex
	db   *badger.DB
	obvs map[librarian.Addr]luigi.Observable
	tipe interface{}
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

		data, err := item.Value()
		if err != nil {
			return errors.Wrap(err, "error getting value")
		}

		if um, ok := v.(librarian.Unmarshaler); ok {
			if t.Kind() != reflect.Ptr {
				v = reflect.ValueOf(v).Elem().Interface()
			}

			err = um.Unmarshal(data)
			err = errors.Wrap(err, "error unmarshaling using custom marshaler")
		} else {
			err = json.Unmarshal(data, v)
			err = errors.Wrap(err, "error unmarshaling using json marshaler")

			if t.Kind() != reflect.Ptr {
				v = reflect.ValueOf(v).Elem().Interface()
			}
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

	err = idx.db.Update(func(txn *badger.Txn) error {
		err := txn.Set([]byte(addr), raw)
		return errors.Wrap(err, "error setting item")
	})
	if err != nil {
		return errors.Wrap(err, "error in badger transaction (update)")
	}

	idx.l.Lock()
	defer idx.l.Unlock()

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
		err = obv.Set(librarian.UnsetValue{addr})
		err = errors.Wrap(err, "error setting value in observable")
	}

	return err
}

type roObv struct {
	luigi.Observable
}

func (obv roObv) Set(interface{}) error {
	return errors.New("read-only observable")
}
