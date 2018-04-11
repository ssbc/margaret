package badger

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/dgraph-io/badger"
	"github.com/pkg/errors"

	"cryptoscope.co/go/librarian"
	"cryptoscope.co/go/librarian/test"
)

func newIdx(name string, tipe interface{}) (librarian.SetterIndex, error) {
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

	return NewIndex(db, tipe), nil
}

func TestBadger(t *testing.T) {
	t.Run("TestSetterIndex", test.TestSetterIndex(newIdx))
}
