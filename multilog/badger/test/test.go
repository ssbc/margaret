package test

import (
	"io/ioutil"
	"os"

	"github.com/dgraph-io/badger"
	"github.com/pkg/errors"

	"go.cryptoscope.co/margaret/codec/json"
	"go.cryptoscope.co/margaret/multilog"
	mlbadger "go.cryptoscope.co/margaret/multilog/badger"
	mltest "go.cryptoscope.co/margaret/multilog/test"
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
