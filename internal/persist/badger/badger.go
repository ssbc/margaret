// SPDX-FileCopyrightText: 2024 The margaret Authors
//
// SPDX-License-Identifier: MIT

package badger

import (
	"fmt"
	"log"

	"github.com/dgraph-io/badger/v4"

	"github.com/ssbc/margaret/v2/internal/persist"
)

type Saver struct {
	db *badger.DB
}

var _ persist.Saver = (*Saver)(nil)

func New(path string) (*Saver, error) {
	opts := badger.DefaultOptions(path).
		WithLogger(nil)
	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("badger: open %s: %w", path, err)
	}
	return &Saver{db: db}, nil
}

func (s *Saver) Close() error {
	return s.db.Close()
}

func (s *Saver) Put(key persist.Key, data []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, data)
	})
}

func (s *Saver) PutMultiple(pairs []persist.KeyValuePair) error {
	wb := s.db.NewWriteBatch()
	for _, kv := range pairs {
		if err := wb.Set(kv.Key, kv.Value); err != nil {
			wb.Cancel()
			return fmt.Errorf("badger: batch set: %w", err)
		}
	}
	if err := wb.Flush(); err != nil {
		return fmt.Errorf("badger: batch flush: %w", err)
	}
	return nil
}

func (s *Saver) Get(key persist.Key) ([]byte, error) {
	var result []byte
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			return persist.ErrNotFound
		}
		if err != nil {
			return err
		}
		result, err = item.ValueCopy(nil)
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s *Saver) List() ([]persist.Key, error) {
	var keys []persist.Key
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			k := it.Item().KeyCopy(nil)
			keys = append(keys, k)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return keys, nil
}

func (s *Saver) Delete(key persist.Key) error {
	return s.db.Update(func(txn *badger.Txn) error {
		err := txn.Delete(key)
		if err == badger.ErrKeyNotFound {
			return nil
		}
		return err
	})
}

// RunGC triggers badger's value log garbage collection.
// Call periodically for long-running processes.
func (s *Saver) RunGC() {
	for {
		err := s.db.RunValueLogGC(0.5)
		if err != nil {
			break
		}
		log.Println("badger: GC cycle completed")
	}
}
