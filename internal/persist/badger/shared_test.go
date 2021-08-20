// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

package badger

import (
	"bytes"
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/dgraph-io/badger/v3"
	"github.com/stretchr/testify/require"
	"go.cryptoscope.co/margaret/internal/persist"
)

func TestSharedBadger(t *testing.T) {
	r := require.New(t)

	path := filepath.Join("testrun", t.Name())
	os.RemoveAll(path)
	os.Mkdir(path, 0700)

	o := badger.DefaultOptions(path)
	db, err := badger.Open(o)
	r.NoError(err)

	// make sure each bucket can use keys as if they are alone
	collidingKey := persist.Key("meins")

	// create two shared instances on the same backing db
	fooBucket, err := NewShared(db, []byte("foo"))
	r.NoError(err)

	barBucket, err := NewShared(db, []byte("bar"))
	r.NoError(err)

	// write two chunks of random data to them
	fooData := make([]byte, 32)
	rand.Read(fooData)
	err = fooBucket.Put(collidingKey, fooData)
	r.NoError(err)

	barData := make([]byte, 32)
	rand.Read(barData)
	err = barBucket.Put(collidingKey, barData)
	r.NoError(err)

	// should both have just one key
	fooKeys, err := fooBucket.List()
	r.NoError(err)
	r.Len(fooKeys, 1)
	r.Equal(collidingKey, fooKeys[0])

	barKeys, err := barBucket.List()
	r.NoError(err)
	r.Len(barKeys, 1)
	r.Equal(collidingKey, barKeys[0])

	// make sure they didnt overwrite each other
	fooGot, err := fooBucket.Get(collidingKey)
	r.NoError(err)
	r.Equal(fooData, fooGot)

	barGot, err := barBucket.Get(collidingKey)
	r.NoError(err)
	r.Equal(barData, barGot)

	// closing a shared should be a noop
	r.NoError(fooBucket.Close())
	r.NoError(barBucket.Close())

	r.False(db.IsClosed())
	r.NoError(db.Close())

	// reopen
	db, err = badger.Open(o)
	r.NoError(err)

	// create two shared instances on the same backing db
	fooBucket, err = NewShared(db, []byte("foo"))
	r.NoError(err)

	fooKeys, err = fooBucket.List()
	r.NoError(err)
	r.Len(fooKeys, 1)
	r.Equal(collidingKey, fooKeys[0])

	// manual lookup
	var hasFoo, hasBar bool
	db.View(func(txn *badger.Txn) error {
		iter := txn.NewIterator(badger.DefaultIteratorOptions)
		defer iter.Close()

		for iter.Rewind(); iter.Valid(); iter.Next() {
			it := iter.Item()

			k := it.Key()

			if bytes.Equal(k, []byte("foomeins")) {
				hasFoo = true
			}
			if bytes.Equal(k, []byte("barmeins")) {
				hasBar = true
			}
		}
		return nil
	})

	r.True(hasFoo, "foo not found")
	r.True(hasBar, "bar not found")
}
