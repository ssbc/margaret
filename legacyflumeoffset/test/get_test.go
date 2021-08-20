// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

package test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	mtest "go.cryptoscope.co/margaret/test"
)

func TestGet(t *testing.T) {
	type testcase struct {
		tipe    interface{}
		values  []interface{}
		offsets []int64
	}

	mkTest := func(fn mtest.NewLogFunc, tc testcase) func(*testing.T) {
		return func(t *testing.T) {
			a := assert.New(t)
			r := require.New(t)

			log, err := fn(t.Name(), tc.tipe)
			r.NoError(err, "error creating log")
			r.NotNil(log, "returned log is nil")

			tc.offsets = make([]int64, len(tc.values))

			// add the values and keep the offsets
			for i, v := range tc.values {
				seq, err := log.Append(v)
				r.NoError(err, "error appending to log")
				t.Logf("entry:%d has seq: %d", i, seq)
				tc.offsets[i] = seq
			}

			// now use the offsets we got to get the values again
			for i, ofst := range tc.offsets {
				got, err := log.Get(ofst)
				a.NoError(err, "error getting value at position", i)
				a.Equal(tc.values[i], got, "value mismatch at position", i)
			}
		}
	}

	tcs := []testcase{
		{
			tipe:   0,
			values: []interface{}{1, 2, 3},
		},

		{
			tipe:   "str",
			values: []interface{}{"abc", "def", "ghi"},
		},
	}

	for name, newLog := range newLogFuncs {

		for i, tc := range tcs {
			t.Run(fmt.Sprintf("%s-%d", name, i), mkTest(newLog, tc))
		}
	}

}
