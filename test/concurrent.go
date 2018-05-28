package test // import "cryptoscope.co/go/margaret/test"

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"cryptoscope.co/go/margaret"
)

func LogTestConcurrent(f NewLogFunc) func(*testing.T) {
	type testcase struct {
		tipe   interface{}
		values []interface{}
		specs  []margaret.QuerySpec
		result []interface{}
	}

	mkTest := func(tc testcase) func(*testing.T) {
		return func(t *testing.T) {
			a := assert.New(t)
			r := require.New(t)

			log, err := f(t.Name(), tc.tipe)
			r.NoError(err, "error creating log")
			r.NotNil(log, "returned log is nil")

			defer func() {
				if namer, ok := log.(interface{ FileName() string }); ok {
					r.NoError(os.Remove(namer.FileName()), "error deleting log after test")
				}
			}()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			seq, err := log.Seq().Value()
			a.NoError(err, "unexpected error")
			a.Equal(margaret.SeqEmpty, seq, "expected empty log")

			var wg sync.WaitGroup
			wg.Add(2)
			go func() {
				defer wg.Done()

				src, err := log.Query(tc.specs...)
				a.NoError(err, "error querying log")

				for i, exp := range tc.result {
					v, err := src.Next(ctx)
					a.NoError(err, "error in call to Next()")
					a.Equal(exp, v, "result doesn't match")

					if t.Failed() {
						t.Log("error in iteration", i)
					}
				}
			}()

			go func() {
				defer wg.Done()

				for i, v := range tc.values {
					err := log.Append(v)
					a.NoError(err, "error appending to log")

					if t.Failed() {
						t.Log("error in iteration", i)
					}
				}
			}()

			wg.Wait()
		}
	}

	tcs := []testcase{
		{
			tipe:   0,
			values: []interface{}{1, 2, 3},
			result: []interface{}{1, 2, 3},
			specs:  []margaret.QuerySpec{margaret.Live(true)},
		},
		{
			tipe:   0,
			values: []interface{}{1, 2, 3},
			result: []interface{}{1, 2},
			specs:  []margaret.QuerySpec{margaret.Live(true), margaret.Limit(2)},
		},
	}

	return func(t_ *testing.T) {
		for i, tc := range tcs {
			t_.Run(fmt.Sprint(i), mkTest(tc))
		}
	}
}
