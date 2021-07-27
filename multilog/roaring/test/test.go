// SPDX-License-Identifier: MIT

package test

import (
	"io/ioutil"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
	"github.com/pkg/errors"

	"go.cryptoscope.co/margaret/internal/persist/fs"
	"go.cryptoscope.co/margaret/multilog"
	"go.cryptoscope.co/margaret/multilog/roaring"
	"go.cryptoscope.co/margaret/multilog/roaring/badger"
	"go.cryptoscope.co/margaret/multilog/roaring/mkv"
	"go.cryptoscope.co/margaret/multilog/roaring/sqlite"
	mltest "go.cryptoscope.co/margaret/multilog/test"
)

func init() {
	mltest.Register("roaring_badger", func(name string, tipe interface{}, testDir string) (multilog.MultiLog, string, error) {
		if testDir == "" {
			var err error
			testDir, err = ioutil.TempDir("", "roarbadger")
			if err != nil {
				return nil, "", errors.Wrap(err, "error creating tempdir")
			}
		}

		badgerMl, err := badger.NewStandalone(testDir)
		return badgerMl, testDir, err
	})

	mltest.Register("roaring_files", func(name string, tipe interface{}, testDir string) (multilog.MultiLog, string, error) {
		if testDir == "" {
			var err error
			testDir, err = ioutil.TempDir("", "roarfiles")
			if err != nil {
				return nil, "", errors.Wrap(err, "error creating tempdir")
			}
		}

		return roaring.NewStore(fs.New(testDir)), testDir, nil
	})

	mltest.Register("roaring_sqlite", func(name string, tipe interface{}, testDir string) (multilog.MultiLog, string, error) {
		if testDir == "" {
			var err error
			testDir, err = ioutil.TempDir("", "roarsqlite")
			if err != nil {
				return nil, "", errors.Wrap(err, "error creating tempdir")
			}
		}
		r, err := sqlite.NewMultiLog(testDir)
		return r, testDir, err
	})

	mltest.Register("roaring_mkv", func(name string, tipe interface{}, testDir string) (multilog.MultiLog, string, error) {
		if testDir == "" {
			var err error
			testDir, err = ioutil.TempDir("", "roar_mkv")
			if err != nil {
				return nil, "", errors.Wrap(err, "error creating tempdir")
			}
			os.MkdirAll(testDir, 0700)
		}
		r, err := mkv.NewMultiLog(filepath.Join(testDir, "mkv.roar"))
		return r, testDir, err
	})
}
