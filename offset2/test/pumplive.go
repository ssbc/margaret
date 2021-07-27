// SPDX-License-Identifier: MIT

package test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.cryptoscope.co/luigi"
	"go.cryptoscope.co/margaret"
	mtest "go.cryptoscope.co/margaret/test"
)

func LogTestPumpLive(f mtest.NewLogFunc) func(*testing.T) {
	type testcase struct {
		tipe interface{}

		specs   []margaret.QuerySpec
		errStr  string
		seqWrap bool
		qryTime int

		//values1 is appended before the query starts
		values1 []interface{}

		// values2 is appended after the query starts
		values2 []interface{}

		// result1 is the expected received output before the query starts
		result1 []interface{}

		// result2 is the expected received output after the query starts
		result2 []interface{}
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
					if iRes != len(tc.result1)+len(tc.result2) {
						t.Errorf("early end of stream at %d instead of %d", iRes, len(tc.result1)+len(tc.result2))
					}

					return nil
				}

				if iRes >= len(tc.result1)+len(tc.result2) {
					t.Fatal("expected end but read value:", v_)
				}

				var v interface{}
				if iRes < len(tc.result1) {
					v = tc.result1[iRes]
				} else {
					v = tc.result2[iRes-len(tc.result1)]
				}

				iRes++

				if tc.errStr == "" {
					if tc.seqWrap {
						sw := v.(margaret.SeqWrapper)
						sw_ := v_.(margaret.SeqWrapper)

						a.Equal(sw.Seq(), sw_.Seq(), "sequence number doesn't match")
						a.Equal(sw.Value(), sw_.Value(), "value doesn't match")
					} else {
						a.Equal(v, v, "values don't match")
					}
				}

				return nil
			})

			// prepare context
			ctx := context.Background()
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			// send first batch
			for i, v := range tc.values1 {
				seq, err := log.Append(v)
				r.NoError(err, "error appending to log")
				r.EqualValues(i, seq, "sequence missmatch")
			}

			// make live query and process it
			wait := make(chan struct{})
			go func() {
				src, err := log.Query(append(tc.specs, margaret.Live(true))...)
				r.NoError(err, "error querying log")

				// process
				err = luigi.Pump(ctx, sink, src)
				a.Equal(context.Canceled, err, "stream copy error")

				// unblock main goroutine
				close(wait)
			}()

			// make sure the goroutine starts first
			time.Sleep(200 * time.Millisecond)

			// send second batch
			for i, v := range tc.values2 {
				seq, err := log.Append(v)
				r.NoError(err, "error appending to log")
				r.EqualValues(len(tc.values1)+i, seq, "sequence missmatch")
			}

			// cancel query processing goroutine
			cancel()

			// wait for query processing goroutine
			<-wait
		}
	}

	tcs := []testcase{
		{
			tipe:    0,
			values1: []interface{}{1, 2, 3},
			values2: []interface{}{4, 5, 6},
			result1: []interface{}{1, 2, 3},
			result2: []interface{}{4, 5, 6},
		},
	}

	return func(t *testing.T) {
		for i, tc := range tcs {
			t.Run(fmt.Sprint(i), mkTest(tc))
		}
	}
}
