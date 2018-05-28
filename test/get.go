package test // import "cryptoscope.co/go/margaret/test"

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"cryptoscope.co/go/margaret"
)

func LogTestGet(f NewLogFunc) func(*testing.T) {
	type testcase struct {
		tipe   interface{}
		values []interface{}
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

			for _, v := range tc.values {
				err := log.Append(v)
				r.NoError(err, "error appending to log")
			}

			for i, v_ := range tc.result {
				v, err := log.Get(margaret.Seq(i))
				a.NoError(err, "error getting value at position", i)
				a.Equal(v, v_, "value mismatch at position", i)
			}
		}
	}

	tcs := []testcase{
		{
			tipe:   0,
			values: []interface{}{1, 2, 3},
			result: []interface{}{1, 2, 3},
		},
	}

	return func(t *testing.T) {
		for i, tc := range tcs {
			t.Run(fmt.Sprint(i), mkTest(tc))
		}
	}
}
