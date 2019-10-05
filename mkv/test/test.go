package test

import (
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"modernc.org/kv"

	"go.cryptoscope.co/librarian"
	libmkv "go.cryptoscope.co/librarian/mkv"
	"go.cryptoscope.co/librarian/test"
)

func init() {
	newSeqSetterIdx := func(name string, tipe interface{}) (librarian.SeqSetterIndex, error) {
		os.RemoveAll("testrun")
		os.MkdirAll("testrun", 0700)
		dir, err := ioutil.TempDir("./testrun", "mkv")
		if err != nil {
			return nil, errors.Wrap(err, "error creating tempdir")
		}

		opts := &kv.Options{}
		db, err := kv.Create(filepath.Join(dir, "db"), opts)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create KV")
		}

		return libmkv.NewIndex(db, tipe), nil
	}

	toSetterIdx := func(f test.NewSeqSetterIndexFunc) test.NewSetterIndexFunc {
		return func(name string, tipe interface{}) (librarian.SetterIndex, error) {
			idx, err := f(name, tipe)
			return idx, err
		}
	}

	test.RegisterSeqSetterIndex("mkv", newSeqSetterIdx)
	test.RegisterSetterIndex("mkv", toSetterIdx(newSeqSetterIdx))
}
