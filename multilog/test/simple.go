package test // import "cryptoscope.co/go/margaret/test"

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"cryptoscope.co/go/luigi"
	"cryptoscope.co/go/margaret"
	mtest "cryptoscope.co/go/margaret/test"
)

func LogTestSimple(f NewLogFunc, g mtest.NewLogFunc) func(*testing.T) {
	type testcase struct {
		tipe    interface{}
		values  []interface{}
		specs   []margaret.QuerySpec
		f multilog.Func
		result  map[librarian.Addr][]interface{}
		errStr  string
		live    bool
		seqWrap bool
	}

	mkTest := func(tc testcase) func(*testing.T) {
		return func(t *testing.T) {
			a := assert.New(t)
			r := require.New(t)

			/*
				- make log
				- make multilog and sink
				- query entire log and pump stream into multilog-sink
				- append values to log
				- check if multilog entries match
			*/

			// make log
			log, err := g(t.Name(), tc.tipe)
			r.NoError(err, "error creating log")
			r.NotNil(log, "returned log is nil")

			defer func() {
				if namer, ok := log.(interface{ FileName() string }); ok {
					a.NoError(os.Remove(namer.FileName()), "error deleting log after test")
				}
			}()

			// make multilog
			mlog := f(t.Name(), tc.tipe)
			sink := multilog.NewSink(mlog, tc.f)
			
			// query entire log and pump stream into multilog-sink
			src, err := log.Query(margaret.Live(true), margaret.Gt(0), margaret.SeqWrap(true))
			r.NoError(err, "error querying log")
			go func() {
				err := luigi.Pump(sink, src)
				r.NoError(err, "error pumping from log to multilog-sink")
				close(wait)
			}()

			// append values
			for i, v := range tc.values {
				seq, err := log.Append(v)
				r.NoError(err, "error appending to log")
				r.Equal(margaret.Seq(i), seq, "sequence missmatch")
			}

			// check if multilog entries match
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
						a.Equal(v, v, "values don't match")
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
				t.Errorf("expected end-of-stream but got %+v", err)
			} else if tc.live && errors.Cause(err) != context.Canceled {
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
			tipe:   0,
			values: []interface{}{1, 2, 3},
			result: []interface{}{1, 2, 3},
		},

		{
			tipe:   0,
			values: []interface{}{1, 2, 3},
			result: []interface{}{2, 3},
			specs:  []margaret.QuerySpec{margaret.Gt(0)},
		},

		{
			tipe:   0,
			values: []interface{}{1, 2, 3},
			result: []interface{}{2, 3},
			specs:  []margaret.QuerySpec{margaret.Gte(1)},
		},

		{
			tipe:   0,
			values: []interface{}{1, 2, 3},
			result: []interface{}{1, 2},
			specs:  []margaret.QuerySpec{margaret.Lt(2)},
		},

		{
			tipe:   0,
			values: []interface{}{1, 2, 3},
			result: []interface{}{1, 2},
			specs:  []margaret.QuerySpec{margaret.Lte(1)},
		},

		{
			tipe:   0,
			values: []interface{}{1, 2, 3},
			result: []interface{}{1, 2},
			specs:  []margaret.QuerySpec{margaret.Limit(2)},
		},

		{
			tipe:   0,
			values: []interface{}{1, 2},
			result: []interface{}{1, 2, 3},
			errStr: "end of stream",
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
				margaret.WrapWithSeq(1, 0),
				margaret.WrapWithSeq(2, 1),
				margaret.WrapWithSeq(3, 2),
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

func count(from, to int64) []interface{} {
	out := make([]interface{}, to-from)
	for i := from; i<to; i++ {
		out[i-from] = i
	}
	return out
}
