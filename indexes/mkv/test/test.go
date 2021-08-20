// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

package test

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"modernc.org/kv"

	"go.cryptoscope.co/margaret/indexes"
	libmkv "go.cryptoscope.co/margaret/indexes/mkv"
	"go.cryptoscope.co/margaret/indexes/test"
)

func init() {
	newSeqSetterIdx := func(name string, tipe interface{}) (indexes.SeqSetterIndex, error) {
		os.RemoveAll("testrun")
		os.MkdirAll("testrun", 0700)
		dir, err := ioutil.TempDir("./testrun", "mkv")
		if err != nil {
			return nil, fmt.Errorf("error creating tempdir: %w", err)
		}

		opts := &kv.Options{}
		db, err := kv.Create(filepath.Join(dir, "db"), opts)
		if err != nil {
			return nil, fmt.Errorf("error opening test database (%s): %w", dir, err)
		}

		return libmkv.NewIndex(db, tipe), nil
	}

	toSetterIdx := func(f test.NewSeqSetterIndexFunc) test.NewSetterIndexFunc {
		return func(name string, tipe interface{}) (indexes.SetterIndex, error) {
			idx, err := f(name, tipe)
			return idx, err
		}
	}

	test.RegisterSeqSetterIndex("mkv", newSeqSetterIdx)
	test.RegisterSetterIndex("mkv", toSetterIdx(newSeqSetterIdx))
}
