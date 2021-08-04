// SPDX-License-Identifier: MIT

package test // import "go.cryptoscope.co/margaret/multilog/test"

import (
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.cryptoscope.co/luigi"
	"go.cryptoscope.co/margaret"
	"go.cryptoscope.co/margaret/indexes"
)

func SubLogTestGet(f NewLogFunc) func(*testing.T) {
	type testcase struct {
		tipe    interface{}
		specs   []margaret.QuerySpec
		values  map[indexes.Addr][]interface{}
		errStr  string
		live    bool
		seqWrap bool
	}

	mkTest := func(tc testcase) func(*testing.T) {
		return func(t *testing.T) {
			a := assert.New(t)
			r := require.New(t)

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

				// check empty
				r.EqualValues(slog.Seq(), margaret.SeqEmpty)

				ev, err := slog.Get(margaret.SeqEmpty)
				r.Error(err)
				r.True(errors.Is(err, luigi.EOS{}), "unexpected error: %s", err)
				r.Nil(ev)

				for i, v := range values {
					seq, err := slog.Append(v)
					r.NoError(err, "error appending to log")
					r.EqualValues(i, seq, "sequence missmatch")
				}

				// check full
				r.EqualValues(len(values)-1, slog.Seq())
			}

			// check if multilog entries match
			for addr, results := range tc.values {
				slog, err := mlog.Get(addr)
				r.NoError(err, "error getting sublog")
				r.NotNil(slog, "retrieved sublog is nil")

				var v_ interface{}
				err = nil

				for seq, v := range results {
					v_, err = slog.Get(int64(seq))
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
				}

				if err != nil && tc.errStr == "" {
					t.Errorf("unexpected error: %+v", err)
				} else if err == nil && tc.errStr != "" {
					t.Errorf("expected error %q but got nil", tc.errStr)
				} else if tc.errStr != "" && err.Error() != tc.errStr {
					t.Errorf("expected error %q but got %q", tc.errStr, err)
				}

				currSeq := slog.Seq()
				v, err := slog.Get(currSeq)
				r.NoError(err)
				r.NotNil(v)
				v, err = slog.Get(currSeq + 1)
				r.Error(err)
				r.Equal(luigi.EOS{}, err)
				r.Nil(v)
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
			tipe:  int64(0),
			specs: []margaret.QuerySpec{margaret.Live(true)},
			live:  true,
			values: map[indexes.Addr][]interface{}{
				indexes.Addr([]byte{0, 0, 0, 2}):  {2, 4, 6, 8, 10, 12, 14, 16, 18},
				indexes.Addr([]byte{0, 0, 0, 3}):  {3, 6, 9, 12, 15, 18},
				indexes.Addr([]byte{0, 0, 0, 4}):  {4, 8, 12, 16},
				indexes.Addr([]byte{0, 0, 0, 5}):  {5, 10, 15},
				indexes.Addr([]byte{0, 0, 0, 6}):  {6, 12, 18},
				indexes.Addr([]byte{0, 0, 0, 7}):  {7, 14},
				indexes.Addr([]byte{0, 0, 0, 8}):  {8, 16},
				indexes.Addr([]byte{0, 0, 0, 9}):  {9, 18},
				indexes.Addr([]byte{0, 0, 0, 10}): {10},
				indexes.Addr([]byte{0, 0, 0, 11}): {11},
				indexes.Addr([]byte{0, 0, 0, 12}): {12},
				indexes.Addr([]byte{0, 0, 0, 12}): {12},
				indexes.Addr([]byte{0, 0, 0, 13}): {13},
				indexes.Addr([]byte{0, 0, 0, 14}): {14},
				indexes.Addr([]byte{0, 0, 0, 15}): {15},
				indexes.Addr([]byte{0, 0, 0, 16}): {16},
				indexes.Addr([]byte{0, 0, 0, 17}): {17},
				indexes.Addr([]byte{0, 0, 0, 18}): {18},
				indexes.Addr([]byte{0, 0, 0, 19}): {19},
			},
		},
	}

	return func(t *testing.T) {
		for i, tc := range tcs {
			t.Run(fmt.Sprint(i), mkTest(tc))
		}
	}
}
