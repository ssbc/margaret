package test

import (
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/pkg/errors"

	_ "github.com/mattn/go-sqlite3"

	"go.cryptoscope.co/margaret/multilog"
	"go.cryptoscope.co/margaret/multilog/roaringfiles"
	mltest "go.cryptoscope.co/margaret/multilog/test"
)

func init() {
	mltest.Register("roaring_files", func(name string, tipe interface{}, testDir string) (multilog.MultiLog, string, error) {
		if testDir == "" {
			var err error
			testDir, err = ioutil.TempDir("", "roarfiles")
			if err != nil {
				return nil, "", errors.Wrap(err, "error creating tempdir")
			}
		}

		return roaringfiles.NewFS(testDir), testDir, nil
	})

	mltest.Register("roaring_sqlite", func(name string, tipe interface{}, testDir string) (multilog.MultiLog, string, error) {
		if testDir == "" {
			var err error
			testDir, err = ioutil.TempDir("", "roarsqlite")
			if err != nil {
				return nil, "", errors.Wrap(err, "error creating tempdir")
			}
		}
		r, err := roaringfiles.NewSQLite(testDir)
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
		r, err := roaringfiles.NewMKV(filepath.Join(testDir, "mkv.roar"))
		return r, testDir, err
	})
}
