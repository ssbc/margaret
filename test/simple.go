// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

package test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ssbc/margaret/v2"
)

func LogTestSimple(f NewLogFunc) func(*testing.T) {
	type testcase struct {
		name   string
		values []Entry
		specs  []margaret.QueryOption
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

			qry := log.Query(tc.specs...)

			var i int
			for _, gotVal := range qry.Iter() {
				r.Less(i, len(tc.result), "more values than expected")
				a.EqualValues(tc.result[i], *gotVal, "values don't match at %d", i)
				i++
			}
			r.NoError(qry.Err(), "query error")
			r.Equal(len(tc.result), i, "result count mismatch")
		}
	}

	tcs := []testcase{
		{
			name:   "simple",
			values: []Entry{1, 2, 3},
			result: []Entry{1, 2, 3},
		},

		{
			name:   "reverse",
			values: []Entry{1, 2, 3, 4, 5},
			result: []Entry{5, 4, 3, 2, 1},
			specs:  []margaret.QueryOption{margaret.Reverse(true)},
		},

		{
			name:   "reverse-false",
			values: []Entry{1, 2, 3, 4, 5},
			result: []Entry{1, 2, 3, 4, 5},
			specs:  []margaret.QueryOption{margaret.Reverse(false)},
		},

		{
			name:   "gt0",
			values: []Entry{1, 2, 3},
			result: []Entry{2, 3},
			specs:  []margaret.QueryOption{margaret.Gt(0)},
		},

		{
			name:   "gte1",
			values: []Entry{1, 2, 3},
			result: []Entry{2, 3},
			specs:  []margaret.QueryOption{margaret.Gte(1)},
		},

		{
			name:   "lt2",
			values: []Entry{1, 2, 3},
			result: []Entry{1, 2},
			specs:  []margaret.QueryOption{margaret.Lt(2)},
		},

		{
			name:   "lte1",
			values: []Entry{1, 2, 3},
			result: []Entry{1, 2},
			specs:  []margaret.QueryOption{margaret.Lte(1)},
		},

		{
			name:   "limit2",
			values: []Entry{1, 2, 3},
			result: []Entry{1, 2},
			specs:  []margaret.QueryOption{margaret.Limit(2)},
		},

		// BUG(cryptix): the iterators needs to be improved to handle these correctly (https://github.com/ssbc/margaret/issues/6)
		// {
		// 	name:   "reverse and gte",
		// 	values: []Entry{1, 2, 3, 4, 5},
		// 	result: []Entry{5, 4, 3, 2},
		// 	specs:  []margaret.QueryOption{margaret.Reverse(true), margaret.Gte(int64(2))},
		// },

		// {
		// 	name:   "reverse and lt",
		// 	values: []Entry{1, 2, 3, 4, 5},
		// 	result: []Entry{3, 2, 1},
		// 	specs:  []margaret.QueryOption{margaret.Reverse(true), margaret.Lt(int64(4))},
		// },

		/* TODO: v2 live mode
		{
			name:   "live",
			values: []Entry{1, 2, 3},
			result: []Entry{1, 2, 3},
			specs:  []margaret.QueryOption{margaret.Live(true)},
		},
		*/
	}

	return func(t *testing.T) {
		for _, tc := range tcs {
			t.Run(tc.name, mkTest(tc))
		}
	}
}
