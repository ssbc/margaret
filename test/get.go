// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

package test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func LogTestGet(f NewLogFunc) func(*testing.T) {
	type testcase struct {
		values []Entry
		result []Entry
	}

	mkTest := func(tc testcase) func(*testing.T) {
		return func(t *testing.T) {
			a := assert.New(t)
			r := require.New(t)

			log, err := f(t.TempDir())
			r.NoError(err, "error creating log")
			r.NotNil(log, "returned log is nil")

			for i, v := range tc.values {
				seq, err := log.Append(&v)
				r.NoError(err, "error appending to log")
				r.EqualValues(i, seq, "sequence mismatch")
			}

			for i, expected := range tc.result {
				v, err := log.Get(int64(i))
				a.NoError(err, "error getting value at position", i)
				a.Equal(expected, *v, "value mismatch at position", i)
			}
		}
	}

	tcs := []testcase{
		{
			values: []Entry{1, 2, 3},
			result: []Entry{1, 2, 3},
		},
	}

	return func(t *testing.T) {
		for i, tc := range tcs {
			t.Run(fmt.Sprint(i), mkTest(tc))
		}
	}
}
