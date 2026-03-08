// SPDX-FileCopyrightText: 2024 The margaret Authors
//
// SPDX-License-Identifier: MIT

package bbolt

import (
	"fmt"
	"os"

	bolt "go.etcd.io/bbolt"

	"github.com/ssbc/margaret/v2/internal/persist"
)

var bucketName = []byte("data")

type Saver struct {
	db *bolt.DB
}

var _ persist.Saver = (*Saver)(nil)

// Option configures a bbolt Saver.
type Option func(*bolt.Options)

// NoSync disables fsync after each commit. This is faster but risks
// data loss on crash. Useful when the caller has its own durability
// model (e.g. roaring multilog's dirty-flag mechanism).
func NoSync(o *bolt.Options) {
	o.NoSync = true
}

func New(path string, opts ...Option) (*Saver, error) {
	os.MkdirAll(path, 0700)
	dbPath := path + "/bolt.db"
	bopts := *bolt.DefaultOptions
	for _, o := range opts {
		o(&bopts)
	}
	db, err := bolt.Open(dbPath, 0600, &bopts)
	if err != nil {
		return nil, fmt.Errorf("bbolt: open %s: %w", dbPath, err)
	}

	// Ensure bucket exists.
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketName)
		return err
	})
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("bbolt: create bucket: %w", err)
	}

	return &Saver{db: db}, nil
}

// Sync forces an fsync on the database file. Only needed with NoSync.
func (s *Saver) Sync() error {
	return s.db.Sync()
}

func (s *Saver) Close() error {
	return s.db.Close()
}

func (s *Saver) Put(key persist.Key, data []byte) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketName).Put(key, data)
	})
}

func (s *Saver) PutMultiple(pairs []persist.KeyValuePair) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		for _, kv := range pairs {
			if err := b.Put(kv.Key, kv.Value); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Saver) Get(key persist.Key) ([]byte, error) {
	var result []byte
	err := s.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketName).Get(key)
		if v == nil {
			return persist.ErrNotFound
		}
		result = make([]byte, len(v))
		copy(result, v)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s *Saver) List() ([]persist.Key, error) {
	var keys []persist.Key
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketName).ForEach(func(k, _ []byte) error {
			key := make([]byte, len(k))
			copy(key, k)
			keys = append(keys, key)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return keys, nil
}

func (s *Saver) Delete(key persist.Key) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketName).Delete(key)
	})
}
