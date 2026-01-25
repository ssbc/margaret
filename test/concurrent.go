// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

package test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ssbc/margaret/v2"
)

func LogTestConcurrent(f NewLogFunc) func(*testing.T) {
	type testcase struct {
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

			seq := log.Seq()
			a.EqualValues(margaret.SeqEmpty, seq, "expected empty log")

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			// Prepend Live to the query options
			opts := append([]margaret.QueryOption{margaret.Live(ctx)}, tc.specs...)
			qry := log.Query(opts...)

			var got []Entry
			var mu sync.Mutex
			done := make(chan struct{})

			go func() {
				defer close(done)
				for _, v := range qry.Iter() {
					mu.Lock()
					got = append(got, *v)
					mu.Unlock()
				}
			}()

			// Append values concurrently
			for i, v := range tc.values {
				seq, err := log.Append(&v)
				a.NoError(err, "error appending to log")
				a.EqualValues(i, seq, "sequence mismatch")
			}

			// Give live iterator time to process, then cancel
			time.Sleep(100 * time.Millisecond)
			cancel()
			<-done

			mu.Lock()
			defer mu.Unlock()
			r.Equal(len(tc.result), len(got), "result count mismatch")
			a.Equal(tc.result, got, "results don't match")
		}
	}

	tcs := []testcase{
		{
			values: []Entry{1, 2, 3},
			result: []Entry{1, 2, 3},
		},
		{
			values: []Entry{1, 2, 3},
			result: []Entry{1, 2},
			specs:  []margaret.QueryOption{margaret.Limit(2)},
		},
	}

	return func(t_ *testing.T) {
		for i, tc := range tcs {
			t_.Run(fmt.Sprint(i), mkTest(tc))
		}
	}
}
