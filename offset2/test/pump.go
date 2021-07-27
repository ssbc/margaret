// SPDX-License-Identifier: MIT

package test

import (
	"context"
	"os"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.cryptoscope.co/luigi"
	"go.cryptoscope.co/margaret"
	mtest "go.cryptoscope.co/margaret/test"
)

func LogTestPump(f mtest.NewLogFunc) func(*testing.T) {
	type testcase struct {
		name    string
		tipe    interface{}
		values  []interface{}
		specs   []margaret.QuerySpec
		result  []interface{}
		errStr  string
		live    bool
		seqWrap bool
		qryTime int
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

			ctx := context.Background()
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			var iRes int
			var closed bool
			sink := luigi.FuncSink(func(ctx context.Context, v_ interface{}, err error) error {
				if err != nil {
					if err != (luigi.EOS{}) {
						t.Log("sink closed with non-EOS error:", err)
					}

					if closed {
						return errors.New("closing closed sink")
					}
					closed = true
					if iRes != len(tc.result) {
						t.Errorf("early end of stream at %d instead of %d", iRes, len(tc.result))
					}

					return nil
				}

				if iRes >= len(tc.result) {
					t.Fatal("expected end but read value:", v_)
				}
				v := tc.result[iRes]
				iRes++

				if tc.errStr == "" {
					if tc.seqWrap {
						sw := v.(margaret.SeqWrapper)
						sw_ := v_.(margaret.SeqWrapper)

						a.Equal(sw.Seq(), sw_.Seq(), "sequence number doesn't match")
						a.Equal(sw.Value(), sw_.Value(), "value doesn't match")
					} else {
						a.Equal(v, v_, "values don't match: %d", iRes)
					}
				}

				// iRes has been incremented so if at the end, it is the same as the length
				if tc.live && iRes == len(tc.result) {
					cancel()
				}
				return nil
			})

			src, err := log.Query(tc.specs...)
			r.NoError(err, "error querying log")

			for i, v := range tc.values {
				seq, err := log.Append(v)
				r.NoError(err, "error appending to log")
				r.EqualValues(i, seq, "sequence missmatch")
			}

			if tc.live {
				cancel()
			}

			err = luigi.Pump(ctx, sink, src)
			if tc.live {
				a.Equal(context.Canceled, err, "stream copy error")
			} else {
				a.NoError(err, "stream copy error")
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
			name:   "gt",
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
			name:   "reverse",
			tipe:   0,
			values: []interface{}{5, 4, 3, 2, 1},
			result: []interface{}{1, 2, 3, 4, 5},
			specs:  []margaret.QuerySpec{margaret.Reverse(true)},
		},

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
	}
}
