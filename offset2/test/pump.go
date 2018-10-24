package test

import (
	"context"
	"fmt"
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
						a.Equal(v, v_, "values don't match")
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
				// this only happens once!
				if i == tc.qryTime {
				}

				seq, err := log.Append(v)
				r.NoError(err, "error appending to log")
				r.Equal(margaret.BaseSeq(i), seq, "sequence missmatch")
			}

			if tc.live {
				cancel()
			}

			err = luigi.Pump(ctx, sink, src)
			if tc.live {
				a.Equal(context.Canceled, err, "stream copy error")
			} else {
				a.Equal(nil, err, "stream copy error")
			}
		}
	}

	tcs := []testcase{
		{
			tipe:   0,
			values: []interface{}{1, 2, 3},
			result: []interface{}{1, 2, 3},
		},

		{
			tipe:   0,
			values: []interface{}{1, 2, 3},
			result: []interface{}{2, 3},
			specs:  []margaret.QuerySpec{margaret.Gt(margaret.BaseSeq(0))},
		},

		{
			tipe:   0,
			values: []interface{}{1, 2, 3},
			result: []interface{}{2, 3},
			specs:  []margaret.QuerySpec{margaret.Gte(margaret.BaseSeq(1))},
		},

		{
			tipe:   0,
			values: []interface{}{1, 2, 3},
			result: []interface{}{1, 2},
			specs:  []margaret.QuerySpec{margaret.Lt(margaret.BaseSeq(2))},
		},

		{
			tipe:   0,
			values: []interface{}{1, 2, 3},
			result: []interface{}{1, 2},
			specs:  []margaret.QuerySpec{margaret.Lte(margaret.BaseSeq(1))},
		},

		{
			tipe:   0,
			values: []interface{}{1, 2, 3},
			result: []interface{}{1, 2},
			specs:  []margaret.QuerySpec{margaret.Limit(2)},
		},

		{
			tipe:   0,
			values: []interface{}{1, 2, 3},
			result: []interface{}{1, 2, 3},
			specs:  []margaret.QuerySpec{margaret.Live(true)},
			live:   true,
		},

		{
			tipe:   0,
			values: []interface{}{1, 2, 3},
			result: []interface{}{
				margaret.WrapWithSeq(1, margaret.BaseSeq(0)),
				margaret.WrapWithSeq(2, margaret.BaseSeq(1)),
				margaret.WrapWithSeq(3, margaret.BaseSeq(2)),
			},
			specs:   []margaret.QuerySpec{margaret.SeqWrap(true)},
			seqWrap: true,
		},
	}

	return func(t *testing.T) {
		for i, tc := range tcs {
			t.Run(fmt.Sprint(i), mkTest(tc))
		}
	}
}
