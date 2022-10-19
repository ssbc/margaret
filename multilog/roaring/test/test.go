// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

package test

import (
	"io/ioutil"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
	"github.com/pkg/errors"

	"github.com/ssbc/margaret/internal/persist/fs"
	"github.com/ssbc/margaret/multilog"
	"github.com/ssbc/margaret/multilog/roaring"
	"github.com/ssbc/margaret/multilog/roaring/badger"
	"github.com/ssbc/margaret/multilog/roaring/mkv"
	"github.com/ssbc/margaret/multilog/roaring/sqlite"
	mltest "github.com/ssbc/margaret/multilog/test"
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
