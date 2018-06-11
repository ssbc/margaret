package test

import (
	"io/ioutil"
	"os"

	"github.com/dgraph-io/badger"
	"github.com/pkg/errors"

	"cryptoscope.co/go/margaret/codec/json"
	"cryptoscope.co/go/margaret/multilog"
	mlbadger "cryptoscope.co/go/margaret/multilog/badger"
	mltest "cryptoscope.co/go/margaret/multilog/test"
	_ "cryptoscope.co/go/margaret/test/all"
)

func init() {
	newMultiLog := func(name string, tipe interface{}) (multilog.MultiLog, error) {
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

		return mlbadger.New(db, tipe, 10, json.New(tipe)), nil
	}

	mltest.Register("badger", newMultiLog)
}
