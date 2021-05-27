// SPDX-License-Identifier: MIT

package test

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/dgraph-io/badger/v3"

	"go.cryptoscope.co/margaret/indexes"
	libadger "go.cryptoscope.co/margaret/indexes/badger"
	"go.cryptoscope.co/margaret/indexes/test"
)

func init() {
	newStandaloneSeqSetterIdx := func(name string, tipe interface{}) (indexes.SeqSetterIndex, error) {
		dir := filepath.Join("testrun", name)
		os.RemoveAll(dir)
		os.MkdirAll(dir, 0700)

		opts := badger.DefaultOptions(dir)

		db, err := badger.Open(opts)
		if err != nil {
			return nil, fmt.Errorf("error opening test database (%s): %w", dir, err)
		}

		return libadger.NewIndex(db, tipe), nil
	}

	var (
		initDB   sync.Once
		sharedDB *badger.DB
	)

	newSharedSeqSetterIdx := func(name string, tipe interface{}) (indexes.SeqSetterIndex, error) {

		initDB.Do(func() {
			dir := filepath.Join("testrun", "badger-shared")
			os.RemoveAll(dir)
			os.MkdirAll(dir, 0700)

			opts := badger.DefaultOptions(dir)

			var err error
			sharedDB, err = badger.Open(opts)
			if err != nil {
				panic(fmt.Errorf("error opening test database (%s): %w", dir, err))
			}
		})

		keyPrefix := make([]byte, 16)
		rand.Read(keyPrefix)

		return libadger.NewIndexWithKeyPrefix(sharedDB, tipe, []byte(hex.EncodeToString(keyPrefix))), nil
	}

	toSetterIdx := func(f test.NewSeqSetterIndexFunc) test.NewSetterIndexFunc {
		return func(name string, tipe interface{}) (indexes.SetterIndex, error) {
			idx, err := f(name, tipe)
			return idx, err
		}
	}

	test.RegisterSeqSetterIndex("badger-standalone", newStandaloneSeqSetterIdx)
	test.RegisterSetterIndex("badger-standalone", toSetterIdx(newStandaloneSeqSetterIdx))

	test.RegisterSeqSetterIndex("badger-shared", newSharedSeqSetterIdx)
	test.RegisterSetterIndex("badger-shared", toSetterIdx(newSharedSeqSetterIdx))
}
