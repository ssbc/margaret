package test

import (
	"io/ioutil"
	"os"

	"github.com/dgraph-io/badger"
	"github.com/pkg/errors"

	"cryptoscope.co/go/librarian"
	libadger "cryptoscope.co/go/librarian/badger"
	"cryptoscope.co/go/librarian/test"
)

func init() {
	newSeqSetterIdx := func(name string, tipe interface{}) (librarian.SeqSetterIndex, error) {
		dir, err := ioutil.TempDir("", "badger")
		if err != nil {
			return nil, errors.Wrap(err, "error creating tempdir")
		}

		defer os.RemoveAll(dir)

		opts := badger.DefaultOptions
		opts.Dir = dir
		opts.ValueDir = dir

		db, err := badger.Open(opts)
		if err != nil {
			return nil, errors.Wrap(err, "error opening database")
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
