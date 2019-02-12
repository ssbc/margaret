package test

import (
	"io/ioutil"

	"github.com/pkg/errors"

	"go.cryptoscope.co/margaret/multilog"
	mltest "go.cryptoscope.co/margaret/multilog/test"
	valbdgr "go.cryptoscope.co/margaret/multilog/roaringfiles"
)

func init() {
	newMultiLog := func(name string, tipe interface{}, testDir string) (multilog.MultiLog, string, error) {
		if testDir == "" {
			var err error
			testDir, err = ioutil.TempDir("", "roarlog")
			if err != nil {
				return nil, "", errors.Wrap(err, "error creating tempdir")
			}
		}

		return valbdgr.New(testDir), testDir, nil
	}

	mltest.Register("value_badger", newMultiLog)
}
