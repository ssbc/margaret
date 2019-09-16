package test

import (
	"io/ioutil"

	"github.com/pkg/errors"

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

		return roaringfiles.NewSQLite(testDir), testDir, nil
	})
}
