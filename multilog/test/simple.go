package test // import "cryptoscope.co/go/margaret/multilog/test"

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"cryptoscope.co/go/librarian"
	"cryptoscope.co/go/luigi"
	"cryptoscope.co/go/margaret"
	"cryptoscope.co/go/margaret/multilog"
	mtest "cryptoscope.co/go/margaret/test"
)

func SinkTestSimple(f NewLogFunc, g mtest.NewLogFunc) func(*testing.T) {
	type testcase struct {
		tipe    interface{}
		values  []interface{}
		specs   []margaret.QuerySpec
		f       multilog.Func
		result  map[librarian.Addr][]interface{}
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
			log = multilog.NameLog(log, "root")

			defer func() {
				if namer, ok := log.(interface{ FileName() string }); ok {
					a.NoError(os.Remove(namer.FileName()), "error deleting log after test")
				}
			}()

			// make multilog
			mlog, err := f(t.Name(), tc.tipe)
			r.NoError(err, "error creating multilog")
				
			sink := multilog.NewSink(mlog, tc.f)

			// query entire log and pump stream into multilog-sink
			src, err := log.Query(margaret.Live(true), margaret.Gte(0), margaret.SeqWrap(true), margaret.Limit(len(tc.values)))
			r.NoError(err, "error querying log")

			go func() {
				err := luigi.Pump(ctx, sink, src)
				r.NoError(err, "error pumping from log to multilog-sink")
			}()

			// append values
			for i, v := range tc.values {
				seq, err := log.Append(v)
				r.NoError(err, "error appending to log")
				r.Equal(margaret.Seq(i), seq, "sequence missmatch")
			}

			// check if multilog entries match
			for addr, results := range tc.result {
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
						case <-time.After(time.Millisecond):
							t.Log("canceling context")
							cancel()
						case <-waiter:
						}
					}()

					v_, err = src.Next(ctx)
					t.Logf("for prefix %x got value %v and error %v - expected %v", addr, v_, err, v)
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
			values: count(1, 20),
			f: func(ctx context.Context, seq multilog.Seq, v interface{}, mlog multilog.MultiLog) error {
				facs := uniq(factorize(int(v.(margaret.Seq))))
				for _, fac := range facs {
					prefixBs := make([]byte, 4)
					binary.BigEndian.PutUint32(prefixBs, uint32(fac))
					prefix := librarian.Addr(prefixBs)
					slog, err := mlog.Get(prefix)
					if err != nil {
						return errors.Wrapf(err, "error getting sublog for prefix %d", fac)
					}

					_, err = slog.Append(seq.Seq())
					if err != nil {
						return errors.Wrapf(err, "error appending to sublog for prefix %d", fac)
					}
				}

				return nil
			},
			result: map[librarian.Addr][]interface{}{
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

func count(from, to int) []interface{} {
	out := make([]interface{}, to-from)
	for i := from; i < to; i++ {
		out[i-from] = margaret.Seq(i)
	}
	return out
}

func factorize(n int) []int {
	var out []int

	for i := 2; n != 1; i++ {
		for n % i == 0 {
			out = append(out, i)
			n = n/i
		}
	}

	return out
}

func uniq(ints []int) []int {
	var out []int

	for _, n := range ints {
		if len(out) == 0 {
			out = append(out, n)
			continue
		}

		if n != out[len(out)-1] {
			out = append(out, n)
		}
	}

	return out
}


