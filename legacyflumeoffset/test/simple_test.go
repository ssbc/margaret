package test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.cryptoscope.co/luigi"
	"go.cryptoscope.co/margaret"
	"go.cryptoscope.co/margaret/codec/json"
	"go.cryptoscope.co/margaret/legacyflumeoffset"
)

func TestSimple(t *testing.T) {

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

			testPath := filepath.Join("testrun", t.Name())
			os.RemoveAll(testPath)

			log, err := legacyflumeoffset.Open(testPath, json.New(tc.tipe))
			r.NoError(err, "error creating log")
			r.NotNil(log, "returned log is nil")

			for i, v := range tc.values {
				seq, err := log.Append(v)
				r.NoError(err, "error appending to log")
				//r.Equal(margaret.BaseSeq(i), seq, "sequence missmatch")
				r.True(int(seq.Seq()) >= i, "increasing sequence")
				t.Log("entry", i, "seq:", seq.Seq())
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

		// {
		// 	name:   "reverse",
		// 	tipe:   0,
		// 	values: []interface{}{1, 2, 3, 4, 5},
		// 	result: []interface{}{5, 4, 3, 2, 1},
		// 	specs:  []margaret.QuerySpec{margaret.Reverse(true)},
		// },

		// {
		// 	name:   "reverse-false",
		// 	tipe:   0,
		// 	values: []interface{}{1, 2, 3, 4, 5},
		// 	result: []interface{}{1, 2, 3, 4, 5},
		// 	specs:  []margaret.QuerySpec{margaret.Reverse(false)},
		// },

		// {
		// 	name:   "gt0",
		// 	tipe:   0,
		// 	values: []interface{}{1, 2, 3},
		// 	result: []interface{}{2, 3},
		// 	specs:  []margaret.QuerySpec{margaret.Gt(margaret.BaseSeq(0))},
		// },

		// {
		// 	name:   "gte1",
		// 	tipe:   0,
		// 	values: []interface{}{1, 2, 3},
		// 	result: []interface{}{2, 3},
		// 	specs:  []margaret.QuerySpec{margaret.Gte(margaret.BaseSeq(13))},
		// },

		// {
		// 	name:   "lt2",
		// 	tipe:   0,
		// 	values: []interface{}{1, 2, 3},
		// 	result: []interface{}{1, 2},
		// 	specs:  []margaret.QuerySpec{margaret.Lt(margaret.BaseSeq(14))},
		// },

		// {
		// 	name:   "lte1",
		// 	tipe:   0,
		// 	values: []interface{}{1, 2, 3},
		// 	result: []interface{}{1, 2},
		// 	specs:  []margaret.QuerySpec{margaret.Lte(margaret.BaseSeq(13))},
		// },

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
		// 	specs:  []margaret.QuerySpec{margaret.Reverse(true), margaret.Gte(margaret.BaseSeq(2))},
		// },

		// {
		// 	name:   "reverse and lt",
		// 	tipe:   0,
		// 	values: []interface{}{1, 2, 3, 4, 5},
		// 	result: []interface{}{3, 2, 1},
		// 	specs:  []margaret.QuerySpec{margaret.Reverse(true), margaret.Lt(margaret.BaseSeq(4))},
		// },

		// {
		// 	name:   "live",
		// 	tipe:   0,
		// 	values: []interface{}{1, 2, 3},
		// 	result: []interface{}{1, 2, 3},
		// 	specs:  []margaret.QuerySpec{margaret.Live(true)},
		// 	live:   true,
		// },

		// {
		// 	name:   "seqWrap",
		// 	tipe:   0,
		// 	values: []interface{}{1, 2, 3},
		// 	result: []interface{}{
		// 		margaret.WrapWithSeq(1, margaret.BaseSeq(0)),
		// 		margaret.WrapWithSeq(2, margaret.BaseSeq(1)),
		// 		margaret.WrapWithSeq(3, margaret.BaseSeq(2)),
		// 	},
		// 	specs:   []margaret.QuerySpec{margaret.SeqWrap(true)},
		// 	seqWrap: true,
		// },
	}

	for _, tc := range tcs {
		t.Run(tc.name, mkTest(tc))
	}

	// t.Run("invalid querys", func(t *testing.T) {
	// 	r := require.New(t)

	// 	// "live and reverse"
	// 	log, err := f(t.Name(), 0)
	// 	r.NoError(err)

	// 	_, err = log.Query(margaret.Live(true), margaret.Reverse(true))
	// 	r.Error(err)
	// 	r.True(strings.Contains(err.Error(), ": can't do reverse and live"))
	// })

}
