// SPDX-License-Identifier: MIT

package test

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/dgraph-io/badger"

	"go.cryptoscope.co/margaret/indexes"
	libadger "go.cryptoscope.co/margaret/indexes/badger"
	"go.cryptoscope.co/margaret/indexes/test"
)

func init() {
	newSeqSetterIdx := func(name string, tipe interface{}) (librarian.SeqSetterIndex, error) {

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

	toSetterIdx := func(f test.NewSeqSetterIndexFunc) test.NewSetterIndexFunc {
		return func(name string, tipe interface{}) (librarian.SetterIndex, error) {
			idx, err := f(name, tipe)
			return idx, err
		}
	}

	test.RegisterSeqSetterIndex("badger", newSeqSetterIdx)
	test.RegisterSetterIndex("badger", toSetterIdx(newSeqSetterIdx))
}
