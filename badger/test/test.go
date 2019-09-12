package test

import (
	"io/ioutil"
	"os"

	"github.com/dgraph-io/badger"
	"github.com/pkg/errors"

	"go.cryptoscope.co/librarian"
	libadger "go.cryptoscope.co/librarian/badger"
	"go.cryptoscope.co/librarian/test"
)

func init() {
	newSeqSetterIdx := func(name string, tipe interface{}) (librarian.SeqSetterIndex, error) {
		dir, err := ioutil.TempDir("", "badger")
		if err != nil {
			return nil, errors.Wrap(err, "error creating tempdir")
		}

		defer os.RemoveAll(dir)

		opts := badger.DefaultOptions(dir)

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
