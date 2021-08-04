// SPDX-License-Identifier: MIT

package test // import "go.cryptoscope.co/margaret/test"

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.cryptoscope.co/luigi"
	"go.cryptoscope.co/margaret"
)

func LogTestSimple(f NewLogFunc) func(*testing.T) {
	type testcase struct {
		name    string
		tipe    interface{}
		values  []interface{}
		specs   []margaret.QuerySpec
		result  []interface{}
		errStr  string
		live    bool
		seqWrap bool
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
					r.NoError(os.RemoveAll(namer.FileName()), "error deleting log after test")
				}
			}()

			for i, v := range tc.values {
				seq, err := log.Append(v)
				r.NoError(err, "error appending to log")
				r.EqualValues(i, seq, "sequence missmatch")

			}

			src, err := log.Query(tc.specs...)
			r.NoError(err, "error querying log")

			ctx := context.Background()
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			waiter := make(chan struct{})
			var v_ interface{}
			err = nil

			for _, v := range tc.result {
				go func() {
					select {
					case <-time.After(time.Millisecond):
						t.Log("canceling context")
						cancel()
					case <-waiter:
					}
				}()

				v_, err = src.Next(ctx)
				if tc.errStr == "" {
					if tc.seqWrap {
						sw := v.(margaret.SeqWrapper)
						sw_ := v_.(margaret.SeqWrapper)

						a.Equal(sw.Seq(), sw_.Seq(), "sequence number doesn't match")
						a.Equal(sw.Value(), sw_.Value(), "value doesn't match")
					} else {
						a.EqualValues(v, v_, "values don't match")
					}
				}
				if err != nil {
					break
				}
				waiter <- struct{}{}
			}

			if err != nil && tc.errStr == "" {
				t.Errorf("unexpected error %+v", err)
			} else if err == nil && tc.errStr != "" {
				t.Errorf("expected error %q but got nil", tc.errStr)
			} else if tc.errStr != "" && err.Error() != tc.errStr {
				t.Errorf("expected error %q but got %q", tc.errStr, err)
			}

			go func() {
				select {
				case <-time.After(time.Millisecond):
					cancel()
				case <-waiter:
				}
			}()

			v, err := src.Next(ctx)
			if !tc.live && !luigi.IsEOS(err) {
				t.Errorf("expected end-of-stream but got %+v (value: %v)", err, v)
			} else if tc.live && !errors.Is(err, context.Canceled) {
				t.Errorf("expected context canceled but got %v, %+v", v, err)
			}

			select {
			case <-time.After(time.Millisecond):
				cancel()
			case waiter <- struct{}{}:
			}
		}
	}

	tcs := []testcase{
		{
			name:   "simple",
			tipe:   0,
			values: []interface{}{1, 2, 3},
			result: []interface{}{1, 2, 3},
		},

		{
			name:   "reverse",
			tipe:   0,
			values: []interface{}{1, 2, 3, 4, 5},
			result: []interface{}{5, 4, 3, 2, 1},
			specs:  []margaret.QuerySpec{margaret.Reverse(true)},
		},

		{
			name:   "reverse-false",
			tipe:   0,
			values: []interface{}{1, 2, 3, 4, 5},
			result: []interface{}{1, 2, 3, 4, 5},
			specs:  []margaret.QuerySpec{margaret.Reverse(false)},
		},

		{
			name:   "gt0",
			tipe:   0,
			values: []interface{}{1, 2, 3},
			result: []interface{}{2, 3},
			specs:  []margaret.QuerySpec{margaret.Gt(0)},
		},

		{
			name:   "gte1",
			tipe:   0,
			values: []interface{}{1, 2, 3},
			result: []interface{}{2, 3},
			specs:  []margaret.QuerySpec{margaret.Gte(1)},
		},

		{
			name:   "lt2",
			tipe:   0,
			values: []interface{}{1, 2, 3},
			result: []interface{}{1, 2},
			specs:  []margaret.QuerySpec{margaret.Lt(2)},
		},

		{
			name:   "lte1",
			tipe:   0,
			values: []interface{}{1, 2, 3},
			result: []interface{}{1, 2},
			specs:  []margaret.QuerySpec{margaret.Lte(1)},
		},

		{
			name:   "limit2",
			tipe:   0,
			values: []interface{}{1, 2, 3},
			result: []interface{}{1, 2},
			specs:  []margaret.QuerySpec{margaret.Limit(2)},
		},

		{
			name:   "EOS",
			tipe:   0,
			values: []interface{}{1, 2},
			result: []interface{}{1, 2, 3},
			errStr: "end of stream",
		},

		// BUG(cryptix): the iterators needs to be improved to handle these correctly (https://github.com/cryptoscope/margaret/issues/6)
		// {
		// 	name:   "reverse and gte",
		// 	tipe:   0,
		// 	values: []interface{}{1, 2, 3, 4, 5},
		// 	result: []interface{}{5, 4, 3, 2},
		// 	specs:  []margaret.QuerySpec{margaret.Reverse(true), margaret.Gte(int64(2))},
		// },

		// {
		// 	name:   "reverse and lt",
		// 	tipe:   0,
		// 	values: []interface{}{1, 2, 3, 4, 5},
		// 	result: []interface{}{3, 2, 1},
		// 	specs:  []margaret.QuerySpec{margaret.Reverse(true), margaret.Lt(int64(4))},
		// },

		{
			name:   "live",
			tipe:   0,
			values: []interface{}{1, 2, 3},
			result: []interface{}{1, 2, 3},
			specs:  []margaret.QuerySpec{margaret.Live(true)},
			live:   true,
		},

		{
			name:   "seqWrap",
			tipe:   0,
			values: []interface{}{1, 2, 3},
			result: []interface{}{
				margaret.WrapWithSeq(1, 0),
				margaret.WrapWithSeq(2, 1),
				margaret.WrapWithSeq(3, 2),
			},
			specs:   []margaret.QuerySpec{margaret.SeqWrap(true)},
			seqWrap: true,
		},
	}

	return func(t *testing.T) {
		for _, tc := range tcs {
			t.Run(tc.name, mkTest(tc))
		}

		t.Run("invalid querys", func(t *testing.T) {
			r := require.New(t)

			// "live and reverse"
			log, err := f(t.Name(), 0)
			r.NoError(err)

			_, err = log.Query(margaret.Live(true), margaret.Reverse(true))
			r.Error(err)
			r.True(strings.Contains(err.Error(), ": can't do reverse and live"))
		})
	}
}
