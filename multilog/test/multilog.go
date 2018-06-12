package test // import "cryptoscope.co/go/margaret/multilog/test"

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"cryptoscope.co/go/librarian"
	"cryptoscope.co/go/luigi"
	"cryptoscope.co/go/margaret"
)

func MultiLogTestSimple(f NewLogFunc) func(*testing.T) {
	type testcase struct {
		tipe    interface{}
		specs   []margaret.QuerySpec
		values  map[librarian.Addr][]interface{}
		errStr  string
		live    bool
		seqWrap bool
	}

	mkTest := func(tc testcase) func(*testing.T) {
		return func(t *testing.T) {
			a := assert.New(t)
			r := require.New(t)
			ctx := context.Background()

			/*
				- make multilog
				- append values to sublogs
				- query all sublogs
				- check if entries match
			*/

			// make multilog
			mlog, err := f(t.Name(), tc.tipe)
			r.NoError(err, "error creating multilog")

			// append values
			for addr, values := range tc.values {
				slog, err := mlog.Get(addr)
				r.NoError(err, "error getting sublog")
				for i, v := range values {
					seq, err := slog.Append(v)
					r.NoError(err, "error appending to log")
					r.Equal(margaret.Seq(i), seq, "sequence missmatch")
				}
			}

			//time.Sleep(time.Second)

			// check if multilog entries match
			for addr, results := range tc.values {
				slog, err := mlog.Get(addr)
				r.NoError(err, "error getting sublog")
				r.NotNil(slog, "retrieved sublog is nil")

				src, err := slog.Query(tc.specs...)
				r.NoError(err, "error querying log")

				ctx, cancel := context.WithCancel(ctx)
				defer cancel()

				waiter := make(chan struct{})
				var v_ interface{}
				err = nil

				for _, v := range results {
					go func() {
						select {
						case <-time.After(20 * time.Millisecond):
							t.Log("canceling context")
							cancel()
						case <-waiter:
						}
					}()

					v_, err = src.Next(ctx)
					// t.Logf("for prefix %x got value %v - expected %v. detailed error: %+v", addr, v_, v, err)
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
					t.Errorf("unexpected error: %+v", err)
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
	}

	tcs := []testcase{
		{
			tipe:   margaret.Seq(0),
			specs: []margaret.QuerySpec{margaret.Live(true)},
			live: true,
			values: map[librarian.Addr][]interface{}{
				librarian.Addr([]byte{0,0,0,2}): []interface{}{2, 4, 6, 8, 10, 12, 14, 16, 18},
				librarian.Addr([]byte{0,0,0,3}): []interface{}{3, 6, 9, 12, 15, 18},
				librarian.Addr([]byte{0,0,0,4}): []interface{}{4, 8, 12, 16},
				librarian.Addr([]byte{0,0,0,5}): []interface{}{5, 10, 15},
				librarian.Addr([]byte{0,0,0,6}): []interface{}{6, 12, 18},
				librarian.Addr([]byte{0,0,0,7}): []interface{}{7, 14},
				librarian.Addr([]byte{0,0,0,8}): []interface{}{8, 16},
				librarian.Addr([]byte{0,0,0,9}): []interface{}{9, 18},
				librarian.Addr([]byte{0,0,0,10}): []interface{}{10},
				librarian.Addr([]byte{0,0,0,11}): []interface{}{11},
				librarian.Addr([]byte{0,0,0,12}): []interface{}{12},
				librarian.Addr([]byte{0,0,0,12}): []interface{}{12},
				librarian.Addr([]byte{0,0,0,13}): []interface{}{13},
				librarian.Addr([]byte{0,0,0,14}): []interface{}{14},
				librarian.Addr([]byte{0,0,0,15}): []interface{}{15},
				librarian.Addr([]byte{0,0,0,16}): []interface{}{16},
				librarian.Addr([]byte{0,0,0,17}): []interface{}{17},
				librarian.Addr([]byte{0,0,0,18}): []interface{}{18},
				librarian.Addr([]byte{0,0,0,19}): []interface{}{19},
			},
		},
	}

	return func(t *testing.T) {
		for i, tc := range tcs {
			t.Run(fmt.Sprint(i), mkTest(tc))
		}
	}
}
