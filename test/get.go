package test // import "cryptoscope.co/go/margaret/test"

import (
	"fmt"
	"testing"

	"cryptoscope.co/go/margaret"
)

func LogTestGet(f func() margaret.Log) func(*testing.T) {
	type testcase struct {
		values []interface{}
		result []interface{}
	}

	mkTest := func(tc testcase) func(*testing.T) {
		return func(t *testing.T) {
			log := f()
			for _, v := range tc.values {
				err := log.Append(v)
				if err != nil {
					t.Error("error appending:", err)
					return
				}
			}

			for i, v_ := range tc.result {
				v, err := log.Get(margaret.Seq(i))
				if err != nil {
					t.Error("error getting:", err)
				}

				if v != v_ {
					t.Errorf("expected %v, got %v", v_, v)
				}
			}
		}
	}

	tcs := []testcase{
		{
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
