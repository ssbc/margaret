// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

package test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ssbc/go-luigi"
	"github.com/ssbc/margaret"
	"github.com/ssbc/margaret/indexes"
	"github.com/ssbc/margaret/multilog"
)

// cblgh: tests to make sure the local fork reproduction that was found when testing against
// peachcloud doesn't reappear
//
// the scenario that detected it:
//  Start a fresh go-sbot (multilog)
//  Post some messages (entries in a sublog)
//  Stop it
//  Some time later: start it again
//  Post more messages (entries in a sublog)
//  Uh-oh the first new message seems to fork, using the previous-to-last message (instead of the last message) as it's previous reference, and with a seqno that is one less than it should be
//
func MultilogTestGetFreshLogCloseThenOpenAgain(f NewLogFunc) func(*testing.T) {
	return func(t *testing.T) {
		a := assert.New(t)
		r := require.New(t)

		mlog, dir, err := f(t.Name(), int64(0), "")
		r.NoError(err)

		// empty yet
		addrs, err := mlog.List()
		r.NoError(err, "error listing mlog")
		r.Len(addrs, 0)

		var addr indexes.Addr = "f23"
		sublog, err := mlog.Get(addr)
		r.NoError(err)
		r.NotNil(sublog)
		// sublog should be empty
		a.EqualValues(margaret.SeqEmpty, sublog.Seq())

		// add some vals
		var vals = []int64{1, 2, 3}
		for i, v := range vals {
			_, err := sublog.Append(v)
			r.NoError(err, "failed to append testVal %d", i)
		}

		a.NotEqual(margaret.SeqEmpty, sublog.Seq())
		a.EqualValues(len(vals)-1, sublog.Seq())

		// sublog was added
		ok, err := multilog.Has(mlog, addr)
		a.NoError(err)
		a.True(ok)

		addrs, err = mlog.List()
		r.NoError(err, "error listing mlog")
		r.Len(addrs, 1)
		r.Equal(addrs[0], addr)

		// reopen
		r.NoError(mlog.Close())
		mlog, dir, err = f(t.Name(), int64(0), dir)
		r.NoError(err)

		ok, err = multilog.Has(mlog, addr)
		a.NoError(err)
		a.True(ok)

		addrs, err = mlog.List()
		r.NoError(err, "error listing mlog")
		r.Len(addrs, 1)

		// now, let's try to add some more values after reopening
		sublog, err = mlog.Get(addr)
		r.NoError(err)
		r.NotNil(sublog)

		for i, v := range vals {
			val, err := sublog.Get(int64(i))
			r.NoError(err)
			r.EqualValues(v, val)
		}

		// add more values
		var moreVals = []int64{4, 5, 6}
		for i, v := range moreVals {
			_, err := sublog.Append(v)
			r.NoError(err, "failed to append testVal %d", i)
		}

		combined := append(vals, moreVals...)

		a.NotEqual(margaret.SeqEmpty, sublog.Seq())
		a.EqualValues(len(combined)-1, sublog.Seq())

		for i, v := range combined {
			val, err := sublog.Get(int64(i))
			r.NoError(err)
			r.EqualValues(v, val)
		}

		// reopen one last time
		r.NoError(mlog.Close())
		mlog, dir, err = f(t.Name(), int64(0), dir)
		r.NoError(err)

		addrs, err = mlog.List()
		r.NoError(err, "error listing mlog")
		r.Len(addrs, 1)

		// sequence numbers are still good
		sublog, err = mlog.Get(addr)
		r.NoError(err)
		r.NotNil(sublog)
		a.EqualValues(len(vals)+len(moreVals)-1, sublog.Seq())

		for i, v := range combined {
			val, err := sublog.Get(int64(i))
			r.NoError(err)
			r.EqualValues(v, val)
		}
	}
}

func MultilogTestAddLogAndListed(f NewLogFunc) func(*testing.T) {
	return func(t *testing.T) {
		a := assert.New(t)
		r := require.New(t)

		mlog, dir, err := f(t.Name(), int64(0), "")
		r.NoError(err)

		// empty yet
		addrs, err := mlog.List()
		r.NoError(err, "error listing mlog")
		r.Len(addrs, 0)

		var addr indexes.Addr = "f23"
		sublog, err := mlog.Get(addr)
		r.NoError(err)
		r.NotNil(sublog)

		// add some vals
		var vals = []int64{1, 2, 3}
		for i, v := range vals {
			_, err := sublog.Append(v)
			r.NoError(err, "failed to append testVal %d", i)
		}

		a.NotEqual(margaret.SeqEmpty, sublog.Seq())

		// sublog was added
		ok, err := multilog.Has(mlog, addr)
		a.NoError(err)
		a.True(ok)

		addrs, err = mlog.List()
		r.NoError(err, "error listing mlog")
		r.Len(addrs, 1)
		r.Equal(addrs[0], addr)

		// reopen
		r.NoError(mlog.Close())
		mlog, dir, err = f(t.Name(), int64(0), dir)
		r.NoError(err)

		ok, err = multilog.Has(mlog, addr)
		a.NoError(err)
		a.True(ok)

		addrs, err = mlog.List()
		r.NoError(err, "error listing mlog")
		r.Len(addrs, 1)

		// empty sublogs do nothing

		for i := 0; i < 10; i++ {
			_, err := mlog.Get(indexes.Addr(fmt.Sprintf("empty%02d", i)))
			r.NoError(err)
		}

		// reopen
		r.NoError(mlog.Close())
		mlog, dir, err = f(t.Name(), int64(0), dir)
		r.NoError(err)

		addrs, err = mlog.List()
		r.NoError(err, "error listing mlog")
		r.Len(addrs, 1)

		// add a log and then delete it
		var delAddr indexes.Addr = "deleteme"
		sublog, err = mlog.Get(delAddr)
		r.NoError(err)
		r.NotNil(sublog)
		vals = []int64{99, 101, 101, 102}
		for i, v := range vals {
			_, err := sublog.Append(v)
			r.NoError(err, "failed to append testVal %d for deletion", i)
		}

		r.NoError(mlog.Flush())

		//  should have the new one
		addrs, err = mlog.List()
		r.NoError(err, "error listing mlog")
		r.Len(addrs, 2)

		// remove it
		err = mlog.Delete(delAddr)
		r.NoError(err, "delete of %s", delAddr)

		// cant use previous handle
		v, err := sublog.Get(0)
		r.Error(err, "get shouldn't work")
		r.Nil(v, "should not return value for deleted sequence")

		errSeq, err := sublog.Append(666)
		r.Error(err, "append shouldn't work")
		r.EqualValues(margaret.SeqSublogDeleted, errSeq, "should not return new sequence")

		src, err := sublog.Query()
		r.Error(err, "query shouldn't work")
		r.Nil(src, "should not return a source")

		// getting fresh, check that it is empty
		sublog, err = mlog.Get(delAddr)
		r.NoError(err)
		r.NotNil(sublog)

		a.Equal(margaret.SeqEmpty, sublog.Seq())

		// one left
		addrs, err = mlog.List()
		r.NoError(err, "error listing mlog")
		r.Len(addrs, 1)

		r.NoError(mlog.Close())
	}
}

func MultiLogTestSimple(f NewLogFunc) func(*testing.T) {
	type testcase struct {
		name    string
		tipe    interface{}
		specs   []margaret.QuerySpec
		values  map[indexes.Addr][]interface{}
		results map[indexes.Addr][]interface{}
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
			mlog, dir, err := f(t.Name(), tc.tipe, "")
			r.NoError(err, "error creating multilog")

			// append values
			for addr, values := range tc.values {
				slog, err := mlog.Get(addr)
				r.NoError(err, "error getting sublog")
				for i, v := range values {
					seq, err := slog.Append(v)
					r.NoError(err, "error appending to log")
					r.EqualValues(i, seq, "sequence missmatch")
				}
			}

			// check Has and List
			has, err := multilog.Has(mlog, indexes.Addr([]byte{0, 0, 0, 19}))
			r.NoError(err)
			r.True(has, "did not find assumed sublog")
			has, err = multilog.Has(mlog, indexes.Addr([]byte{0, 0, 0, 20}))
			r.NoError(err)
			r.False(has, "did find unassumed sublog")

			knownLogs, err := mlog.List()
			r.NoError(err, "error calling List")
			r.Len(knownLogs, len(tc.values))

			hasAddr := func(addr indexes.Addr) bool {
				for _, a := range knownLogs {
					if a == addr {
						return true
					}
				}
				return false
			}

			for addr := range tc.values {
				a.True(hasAddr(addr), "failed to find %s in List()", addr)
			}

			// check if multilog entries match
			for addr, results := range tc.results {
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

				for i, v := range results {
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

							a.Equal(sw.Seq(), sw_.Seq(), "sequence number doesn't match - result %d", i)
							a.Equal(sw.Value(), sw_.Value(), "value doesn't match - result %d", i)
						} else {
							a.EqualValues(v, v_, "values don't match - result %d", i)
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
				} else if tc.live && !errors.Is(err, context.Canceled) {
					t.Errorf("expected context canceled but got %v, %+v", v, err)
				}

				select {
				case <-time.After(time.Millisecond):
					cancel()
				case waiter <- struct{}{}:
				}
			}

			r.NoError(mlog.Close(), "failed to close testlog")

			if t.Failed() {
				t.Log("db location:", dir)
			} else {
				os.RemoveAll(dir)
			}
		}
	}

	tcs := []testcase{
		{
			name:  "simple all",
			tipe:  int64(0),
			specs: []margaret.QuerySpec{margaret.Live(true)},
			live:  true,
			values: map[indexes.Addr][]interface{}{
				indexes.Addr([]byte{0, 0, 0, 2}):  []interface{}{2, 4, 6, 8, 10, 12, 14, 16, 18},
				indexes.Addr([]byte{0, 0, 0, 3}):  []interface{}{3, 6, 9, 12, 15, 18},
				indexes.Addr([]byte{0, 0, 0, 4}):  []interface{}{4, 8, 12, 16},
				indexes.Addr([]byte{0, 0, 0, 5}):  []interface{}{5, 10, 15},
				indexes.Addr([]byte{0, 0, 0, 6}):  []interface{}{6, 12, 18},
				indexes.Addr([]byte{0, 0, 0, 7}):  []interface{}{7, 14},
				indexes.Addr([]byte{0, 0, 0, 8}):  []interface{}{8, 16},
				indexes.Addr([]byte{0, 0, 0, 9}):  []interface{}{9, 18},
				indexes.Addr([]byte{0, 0, 0, 10}): []interface{}{10},
				indexes.Addr([]byte{0, 0, 0, 11}): []interface{}{11},
				indexes.Addr([]byte{0, 0, 0, 12}): []interface{}{12},
				indexes.Addr([]byte{0, 0, 0, 12}): []interface{}{12},
				indexes.Addr([]byte{0, 0, 0, 13}): []interface{}{13},
				indexes.Addr([]byte{0, 0, 0, 14}): []interface{}{14},
				indexes.Addr([]byte{0, 0, 0, 15}): []interface{}{15},
				indexes.Addr([]byte{0, 0, 0, 16}): []interface{}{16},
				indexes.Addr([]byte{0, 0, 0, 17}): []interface{}{17},
				indexes.Addr([]byte{0, 0, 0, 18}): []interface{}{18},
				indexes.Addr([]byte{0, 0, 0, 19}): []interface{}{19},
			},
			results: map[indexes.Addr][]interface{}{
				indexes.Addr([]byte{0, 0, 0, 2}):  []interface{}{2, 4, 6, 8, 10, 12, 14, 16, 18},
				indexes.Addr([]byte{0, 0, 0, 3}):  []interface{}{3, 6, 9, 12, 15, 18},
				indexes.Addr([]byte{0, 0, 0, 4}):  []interface{}{4, 8, 12, 16},
				indexes.Addr([]byte{0, 0, 0, 5}):  []interface{}{5, 10, 15},
				indexes.Addr([]byte{0, 0, 0, 6}):  []interface{}{6, 12, 18},
				indexes.Addr([]byte{0, 0, 0, 7}):  []interface{}{7, 14},
				indexes.Addr([]byte{0, 0, 0, 8}):  []interface{}{8, 16},
				indexes.Addr([]byte{0, 0, 0, 9}):  []interface{}{9, 18},
				indexes.Addr([]byte{0, 0, 0, 10}): []interface{}{10},
				indexes.Addr([]byte{0, 0, 0, 11}): []interface{}{11},
				indexes.Addr([]byte{0, 0, 0, 12}): []interface{}{12},
				indexes.Addr([]byte{0, 0, 0, 12}): []interface{}{12},
				indexes.Addr([]byte{0, 0, 0, 13}): []interface{}{13},
				indexes.Addr([]byte{0, 0, 0, 14}): []interface{}{14},
				indexes.Addr([]byte{0, 0, 0, 15}): []interface{}{15},
				indexes.Addr([]byte{0, 0, 0, 16}): []interface{}{16},
				indexes.Addr([]byte{0, 0, 0, 17}): []interface{}{17},
				indexes.Addr([]byte{0, 0, 0, 18}): []interface{}{18},
				indexes.Addr([]byte{0, 0, 0, 19}): []interface{}{19},
			},
		},

		// BUG(cryptix): roaring does not implement reverse right now and just throws an error (https://github.com/ssbc/margaret/issues/7)
		{
			name:  "reverse",
			tipe:  int64(0),
			specs: []margaret.QuerySpec{margaret.Reverse(true)},
			values: map[indexes.Addr][]interface{}{
				indexes.Addr([]byte{0, 0, 0, 2}):  []interface{}{2, 4, 6, 8, 10, 12, 14, 16, 18},
				indexes.Addr([]byte{0, 0, 0, 3}):  []interface{}{3, 6, 9, 12, 15, 18},
				indexes.Addr([]byte{0, 0, 0, 4}):  []interface{}{4, 8, 12, 16},
				indexes.Addr([]byte{0, 0, 0, 5}):  []interface{}{5, 10, 15},
				indexes.Addr([]byte{0, 0, 0, 6}):  []interface{}{6, 12, 18},
				indexes.Addr([]byte{0, 0, 0, 7}):  []interface{}{7, 14},
				indexes.Addr([]byte{0, 0, 0, 8}):  []interface{}{8, 16},
				indexes.Addr([]byte{0, 0, 0, 9}):  []interface{}{9, 18},
				indexes.Addr([]byte{0, 0, 0, 10}): []interface{}{10},
				indexes.Addr([]byte{0, 0, 0, 11}): []interface{}{11},
				indexes.Addr([]byte{0, 0, 0, 12}): []interface{}{12},
				indexes.Addr([]byte{0, 0, 0, 12}): []interface{}{12},
				indexes.Addr([]byte{0, 0, 0, 13}): []interface{}{13},
				indexes.Addr([]byte{0, 0, 0, 14}): []interface{}{14},
				indexes.Addr([]byte{0, 0, 0, 15}): []interface{}{15},
				indexes.Addr([]byte{0, 0, 0, 16}): []interface{}{16},
				indexes.Addr([]byte{0, 0, 0, 17}): []interface{}{17},
				indexes.Addr([]byte{0, 0, 0, 18}): []interface{}{18},
				indexes.Addr([]byte{0, 0, 0, 19}): []interface{}{19},
			},
			results: map[indexes.Addr][]interface{}{
				indexes.Addr([]byte{0, 0, 0, 2}):  []interface{}{18, 16, 14, 12, 10, 8, 6, 4, 2},
				indexes.Addr([]byte{0, 0, 0, 3}):  []interface{}{18, 15, 12, 9, 6, 3},
				indexes.Addr([]byte{0, 0, 0, 4}):  []interface{}{16, 12, 8, 4},
				indexes.Addr([]byte{0, 0, 0, 5}):  []interface{}{15, 10, 5},
				indexes.Addr([]byte{0, 0, 0, 6}):  []interface{}{18, 12, 6},
				indexes.Addr([]byte{0, 0, 0, 7}):  []interface{}{14, 7},
				indexes.Addr([]byte{0, 0, 0, 8}):  []interface{}{16, 8},
				indexes.Addr([]byte{0, 0, 0, 9}):  []interface{}{18, 9},
				indexes.Addr([]byte{0, 0, 0, 10}): []interface{}{10},
				indexes.Addr([]byte{0, 0, 0, 11}): []interface{}{11},
				indexes.Addr([]byte{0, 0, 0, 12}): []interface{}{12},
				indexes.Addr([]byte{0, 0, 0, 12}): []interface{}{12},
				indexes.Addr([]byte{0, 0, 0, 13}): []interface{}{13},
				indexes.Addr([]byte{0, 0, 0, 14}): []interface{}{14},
				indexes.Addr([]byte{0, 0, 0, 15}): []interface{}{15},
				indexes.Addr([]byte{0, 0, 0, 16}): []interface{}{16},
				indexes.Addr([]byte{0, 0, 0, 17}): []interface{}{17},
				indexes.Addr([]byte{0, 0, 0, 18}): []interface{}{18},
				indexes.Addr([]byte{0, 0, 0, 19}): []interface{}{19},
			},
		},

		{
			name:  "limit1",
			tipe:  int64(0),
			specs: []margaret.QuerySpec{margaret.Limit(1)},
			values: map[indexes.Addr][]interface{}{
				indexes.Addr([]byte{0, 0, 0, 2}):  []interface{}{2, 4, 6, 8, 10, 12, 14, 16, 18},
				indexes.Addr([]byte{0, 0, 0, 3}):  []interface{}{3, 6, 9, 12, 15, 18},
				indexes.Addr([]byte{0, 0, 0, 4}):  []interface{}{4, 8, 12, 16},
				indexes.Addr([]byte{0, 0, 0, 5}):  []interface{}{5, 10, 15},
				indexes.Addr([]byte{0, 0, 0, 6}):  []interface{}{6, 12, 18},
				indexes.Addr([]byte{0, 0, 0, 7}):  []interface{}{7, 14},
				indexes.Addr([]byte{0, 0, 0, 8}):  []interface{}{8, 16},
				indexes.Addr([]byte{0, 0, 0, 9}):  []interface{}{9, 18},
				indexes.Addr([]byte{0, 0, 0, 10}): []interface{}{10},
				indexes.Addr([]byte{0, 0, 0, 11}): []interface{}{11},
				indexes.Addr([]byte{0, 0, 0, 12}): []interface{}{12},
				indexes.Addr([]byte{0, 0, 0, 12}): []interface{}{12},
				indexes.Addr([]byte{0, 0, 0, 13}): []interface{}{13},
				indexes.Addr([]byte{0, 0, 0, 14}): []interface{}{14},
				indexes.Addr([]byte{0, 0, 0, 15}): []interface{}{15},
				indexes.Addr([]byte{0, 0, 0, 16}): []interface{}{16},
				indexes.Addr([]byte{0, 0, 0, 17}): []interface{}{17},
				indexes.Addr([]byte{0, 0, 0, 18}): []interface{}{18},
				indexes.Addr([]byte{0, 0, 0, 19}): []interface{}{19},
			},
			results: map[indexes.Addr][]interface{}{
				indexes.Addr([]byte{0, 0, 0, 2}):  []interface{}{2},
				indexes.Addr([]byte{0, 0, 0, 3}):  []interface{}{3},
				indexes.Addr([]byte{0, 0, 0, 4}):  []interface{}{4},
				indexes.Addr([]byte{0, 0, 0, 5}):  []interface{}{5},
				indexes.Addr([]byte{0, 0, 0, 6}):  []interface{}{6},
				indexes.Addr([]byte{0, 0, 0, 7}):  []interface{}{7},
				indexes.Addr([]byte{0, 0, 0, 8}):  []interface{}{8},
				indexes.Addr([]byte{0, 0, 0, 9}):  []interface{}{9},
				indexes.Addr([]byte{0, 0, 0, 10}): []interface{}{10},
				indexes.Addr([]byte{0, 0, 0, 11}): []interface{}{11},
				indexes.Addr([]byte{0, 0, 0, 12}): []interface{}{12},
				indexes.Addr([]byte{0, 0, 0, 12}): []interface{}{12},
				indexes.Addr([]byte{0, 0, 0, 13}): []interface{}{13},
				indexes.Addr([]byte{0, 0, 0, 14}): []interface{}{14},
				indexes.Addr([]byte{0, 0, 0, 15}): []interface{}{15},
				indexes.Addr([]byte{0, 0, 0, 16}): []interface{}{16},
				indexes.Addr([]byte{0, 0, 0, 17}): []interface{}{17},
				indexes.Addr([]byte{0, 0, 0, 18}): []interface{}{18},
				indexes.Addr([]byte{0, 0, 0, 19}): []interface{}{19},
			},
		},

		{
			name:  "live and gte1",
			tipe:  int64(0),
			specs: []margaret.QuerySpec{margaret.Live(true), margaret.Gte(int64(1))},
			live:  true,
			values: map[indexes.Addr][]interface{}{
				indexes.Addr([]byte{0, 0, 0, 2}):  []interface{}{2, 4, 6, 8, 10, 12, 14, 16, 18},
				indexes.Addr([]byte{0, 0, 0, 3}):  []interface{}{3, 6, 9, 12, 15, 18},
				indexes.Addr([]byte{0, 0, 0, 4}):  []interface{}{4, 8, 12, 16},
				indexes.Addr([]byte{0, 0, 0, 5}):  []interface{}{5, 10, 15},
				indexes.Addr([]byte{0, 0, 0, 6}):  []interface{}{6, 12, 18},
				indexes.Addr([]byte{0, 0, 0, 7}):  []interface{}{7, 14},
				indexes.Addr([]byte{0, 0, 0, 8}):  []interface{}{8, 16},
				indexes.Addr([]byte{0, 0, 0, 9}):  []interface{}{9, 18},
				indexes.Addr([]byte{0, 0, 0, 10}): []interface{}{10},
				indexes.Addr([]byte{0, 0, 0, 11}): []interface{}{11},
				indexes.Addr([]byte{0, 0, 0, 12}): []interface{}{12},
				indexes.Addr([]byte{0, 0, 0, 12}): []interface{}{12},
				indexes.Addr([]byte{0, 0, 0, 13}): []interface{}{13},
				indexes.Addr([]byte{0, 0, 0, 14}): []interface{}{14},
				indexes.Addr([]byte{0, 0, 0, 15}): []interface{}{15},
				indexes.Addr([]byte{0, 0, 0, 16}): []interface{}{16},
				indexes.Addr([]byte{0, 0, 0, 17}): []interface{}{17},
				indexes.Addr([]byte{0, 0, 0, 18}): []interface{}{18},
				indexes.Addr([]byte{0, 0, 0, 19}): []interface{}{19},
			},
			results: map[indexes.Addr][]interface{}{
				indexes.Addr([]byte{0, 0, 0, 2}):  []interface{}{4, 6, 8, 10, 12, 14, 16, 18},
				indexes.Addr([]byte{0, 0, 0, 3}):  []interface{}{6, 9, 12, 15, 18},
				indexes.Addr([]byte{0, 0, 0, 4}):  []interface{}{8, 12, 16},
				indexes.Addr([]byte{0, 0, 0, 5}):  []interface{}{10, 15},
				indexes.Addr([]byte{0, 0, 0, 6}):  []interface{}{12, 18},
				indexes.Addr([]byte{0, 0, 0, 7}):  []interface{}{14},
				indexes.Addr([]byte{0, 0, 0, 8}):  []interface{}{16},
				indexes.Addr([]byte{0, 0, 0, 9}):  []interface{}{18},
				indexes.Addr([]byte{0, 0, 0, 10}): []interface{}{},
				indexes.Addr([]byte{0, 0, 0, 11}): []interface{}{},
				indexes.Addr([]byte{0, 0, 0, 12}): []interface{}{},
				indexes.Addr([]byte{0, 0, 0, 12}): []interface{}{},
				indexes.Addr([]byte{0, 0, 0, 13}): []interface{}{},
				indexes.Addr([]byte{0, 0, 0, 14}): []interface{}{},
				indexes.Addr([]byte{0, 0, 0, 15}): []interface{}{},
				indexes.Addr([]byte{0, 0, 0, 16}): []interface{}{},
				indexes.Addr([]byte{0, 0, 0, 17}): []interface{}{},
				indexes.Addr([]byte{0, 0, 0, 18}): []interface{}{},
				indexes.Addr([]byte{0, 0, 0, 19}): []interface{}{},
			},
		},

		{
			name:  "lte3",
			tipe:  int64(0),
			specs: []margaret.QuerySpec{margaret.Lte(int64(3))},
			values: map[indexes.Addr][]interface{}{
				indexes.Addr([]byte{0, 0, 0, 2}):  []interface{}{2, 4, 6, 8, 10, 12, 14, 16, 18},
				indexes.Addr([]byte{0, 0, 0, 3}):  []interface{}{3, 6, 9, 12, 15, 18},
				indexes.Addr([]byte{0, 0, 0, 4}):  []interface{}{4, 8, 12, 16},
				indexes.Addr([]byte{0, 0, 0, 5}):  []interface{}{5, 10, 15},
				indexes.Addr([]byte{0, 0, 0, 6}):  []interface{}{6, 12, 18},
				indexes.Addr([]byte{0, 0, 0, 7}):  []interface{}{7, 14},
				indexes.Addr([]byte{0, 0, 0, 8}):  []interface{}{8, 16},
				indexes.Addr([]byte{0, 0, 0, 9}):  []interface{}{9, 18},
				indexes.Addr([]byte{0, 0, 0, 10}): []interface{}{10},
				indexes.Addr([]byte{0, 0, 0, 11}): []interface{}{11},
				indexes.Addr([]byte{0, 0, 0, 12}): []interface{}{12},
				indexes.Addr([]byte{0, 0, 0, 12}): []interface{}{12},
				indexes.Addr([]byte{0, 0, 0, 13}): []interface{}{13},
				indexes.Addr([]byte{0, 0, 0, 14}): []interface{}{14},
				indexes.Addr([]byte{0, 0, 0, 15}): []interface{}{15},
				indexes.Addr([]byte{0, 0, 0, 16}): []interface{}{16},
				indexes.Addr([]byte{0, 0, 0, 17}): []interface{}{17},
				indexes.Addr([]byte{0, 0, 0, 18}): []interface{}{18},
				indexes.Addr([]byte{0, 0, 0, 19}): []interface{}{19},
			},
			results: map[indexes.Addr][]interface{}{
				indexes.Addr([]byte{0, 0, 0, 2}):  []interface{}{2, 4, 6, 8},
				indexes.Addr([]byte{0, 0, 0, 3}):  []interface{}{3, 6, 9, 12},
				indexes.Addr([]byte{0, 0, 0, 4}):  []interface{}{4, 8, 12, 16},
				indexes.Addr([]byte{0, 0, 0, 5}):  []interface{}{5, 10, 15},
				indexes.Addr([]byte{0, 0, 0, 6}):  []interface{}{6, 12, 18},
				indexes.Addr([]byte{0, 0, 0, 7}):  []interface{}{7, 14},
				indexes.Addr([]byte{0, 0, 0, 8}):  []interface{}{8, 16},
				indexes.Addr([]byte{0, 0, 0, 9}):  []interface{}{9, 18},
				indexes.Addr([]byte{0, 0, 0, 10}): []interface{}{10},
				indexes.Addr([]byte{0, 0, 0, 11}): []interface{}{11},
				indexes.Addr([]byte{0, 0, 0, 12}): []interface{}{12},
				indexes.Addr([]byte{0, 0, 0, 12}): []interface{}{12},
				indexes.Addr([]byte{0, 0, 0, 13}): []interface{}{13},
				indexes.Addr([]byte{0, 0, 0, 14}): []interface{}{14},
				indexes.Addr([]byte{0, 0, 0, 15}): []interface{}{15},
				indexes.Addr([]byte{0, 0, 0, 16}): []interface{}{16},
				indexes.Addr([]byte{0, 0, 0, 17}): []interface{}{17},
				indexes.Addr([]byte{0, 0, 0, 18}): []interface{}{18},
				indexes.Addr([]byte{0, 0, 0, 19}): []interface{}{19},
			},
		},

		{
			name:  "lt3",
			tipe:  int64(0),
			specs: []margaret.QuerySpec{margaret.Lt(int64(3))},
			values: map[indexes.Addr][]interface{}{
				indexes.Addr([]byte{0, 0, 0, 2}):  []interface{}{2, 4, 6, 8, 10, 12, 14, 16, 18},
				indexes.Addr([]byte{0, 0, 0, 3}):  []interface{}{3, 6, 9, 12, 15, 18},
				indexes.Addr([]byte{0, 0, 0, 4}):  []interface{}{4, 8, 12, 16},
				indexes.Addr([]byte{0, 0, 0, 5}):  []interface{}{5, 10, 15},
				indexes.Addr([]byte{0, 0, 0, 6}):  []interface{}{6, 12, 18},
				indexes.Addr([]byte{0, 0, 0, 7}):  []interface{}{7, 14},
				indexes.Addr([]byte{0, 0, 0, 8}):  []interface{}{8, 16},
				indexes.Addr([]byte{0, 0, 0, 9}):  []interface{}{9, 18},
				indexes.Addr([]byte{0, 0, 0, 10}): []interface{}{10},
				indexes.Addr([]byte{0, 0, 0, 11}): []interface{}{11},
				indexes.Addr([]byte{0, 0, 0, 12}): []interface{}{12},
				indexes.Addr([]byte{0, 0, 0, 12}): []interface{}{12},
				indexes.Addr([]byte{0, 0, 0, 13}): []interface{}{13},
				indexes.Addr([]byte{0, 0, 0, 14}): []interface{}{14},
				indexes.Addr([]byte{0, 0, 0, 15}): []interface{}{15},
				indexes.Addr([]byte{0, 0, 0, 16}): []interface{}{16},
				indexes.Addr([]byte{0, 0, 0, 17}): []interface{}{17},
				indexes.Addr([]byte{0, 0, 0, 18}): []interface{}{18},
				indexes.Addr([]byte{0, 0, 0, 19}): []interface{}{19},
			},
			results: map[indexes.Addr][]interface{}{
				indexes.Addr([]byte{0, 0, 0, 2}):  []interface{}{2, 4, 6},
				indexes.Addr([]byte{0, 0, 0, 3}):  []interface{}{3, 6, 9},
				indexes.Addr([]byte{0, 0, 0, 4}):  []interface{}{4, 8, 12},
				indexes.Addr([]byte{0, 0, 0, 5}):  []interface{}{5, 10, 15},
				indexes.Addr([]byte{0, 0, 0, 6}):  []interface{}{6, 12, 18},
				indexes.Addr([]byte{0, 0, 0, 7}):  []interface{}{7, 14},
				indexes.Addr([]byte{0, 0, 0, 8}):  []interface{}{8, 16},
				indexes.Addr([]byte{0, 0, 0, 9}):  []interface{}{9, 18},
				indexes.Addr([]byte{0, 0, 0, 10}): []interface{}{10},
				indexes.Addr([]byte{0, 0, 0, 11}): []interface{}{11},
				indexes.Addr([]byte{0, 0, 0, 12}): []interface{}{12},
				indexes.Addr([]byte{0, 0, 0, 12}): []interface{}{12},
				indexes.Addr([]byte{0, 0, 0, 13}): []interface{}{13},
				indexes.Addr([]byte{0, 0, 0, 14}): []interface{}{14},
				indexes.Addr([]byte{0, 0, 0, 15}): []interface{}{15},
				indexes.Addr([]byte{0, 0, 0, 16}): []interface{}{16},
				indexes.Addr([]byte{0, 0, 0, 17}): []interface{}{17},
				indexes.Addr([]byte{0, 0, 0, 18}): []interface{}{18},
				indexes.Addr([]byte{0, 0, 0, 19}): []interface{}{19},
			},
		},
	}

	return func(t *testing.T) {
		for _, tc := range tcs {
			t.Run(tc.name, mkTest(tc))
		}
	}
}
