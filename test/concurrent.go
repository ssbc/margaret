package test // import "cryptoscope.co/go/margaret/test"

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"cryptoscope.co/go/margaret"
)

func LogTestConcurrent(f func() margaret.Log) func(*testing.T) {
	type testcase struct {
		values []interface{}
		specs  []margaret.QuerySpec
		result []interface{}
	}

	mkTest := func(tc testcase) func(*testing.T) {
		return func(t *testing.T) {
			log := f()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			seq, err := log.Seq().Value()
			if err != nil {
				t.Error("unexpected error", err)
			}
			if seq != margaret.SeqEmpty {
				t.Errorf("expected empty log but got seq=%d", seq)
			}

			var wg sync.WaitGroup
			wg.Add(2)
			go func() {
				defer wg.Done()

				src, err := log.Query(tc.specs...)
				if err != nil {
					t.Errorf("expected nil error but got %s", err)
				}

				for i, exp := range tc.result {
					v, err := src.Next(ctx)
					if err != nil {
						t.Errorf("unexpected error %s", err)
					}

					if v != exp {
						t.Errorf("expected result %v but got %v", exp, v)
					}

					if t.Failed() {
						t.Log("error in iteration", i)
					}
				}
			}()

			go func() {
				defer wg.Done()

				for i, v := range tc.values {
					err := log.Append(v)
					if err != nil {
						t.Errorf("unexpected error %s", err)
					}

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
			values: []interface{}{1, 2, 3},
			result: []interface{}{1, 2, 3},
			specs:  []margaret.QuerySpec{margaret.Live(true)},
		},
		{
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
