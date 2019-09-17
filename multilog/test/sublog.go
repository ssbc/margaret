package test // import "go.cryptoscope.co/margaret/multilog/test"

import (
	"fmt"
	"os"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.cryptoscope.co/librarian"
	"go.cryptoscope.co/luigi"
	"go.cryptoscope.co/margaret"
)

func SubLogTestGet(f NewLogFunc) func(*testing.T) {
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
				sv, err := slog.Seq().Value()
				r.NoError(err, "error getting sublog sequence")
				seq, ok := sv.(margaret.Seq)
				r.True(ok, "wrong type:%T", sv)
				r.EqualValues(seq.Seq(), margaret.SeqEmpty)

				ev, err := slog.Get(margaret.SeqEmpty)
				r.Error(err)
				r.Equal(luigi.EOS{}, errors.Cause(err))
				r.Nil(ev)

				for i, v := range values {
					seq, err := slog.Append(v)
					r.NoError(err, "error appending to log")
					r.Equal(margaret.BaseSeq(i), seq, "sequence missmatch")
				}

				// check full
				sv, err = slog.Seq().Value()
				r.NoError(err, "error getting sublog sequence")
				seq, ok = sv.(margaret.Seq)
				r.True(ok, "wrong type:%T", sv)
				r.EqualValues(len(values)-1, seq.Seq())
			}

			// check if multilog entries match
			for addr, results := range tc.values {
				slog, err := mlog.Get(addr)
				r.NoError(err, "error getting sublog")
				r.NotNil(slog, "retrieved sublog is nil")

				var v_ interface{}
				err = nil

				for seq, v := range results {
					v_, err = slog.Get(margaret.BaseSeq(seq))
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

				currV, err := slog.Seq().Value()
				r.NoError(err)
				currSeq := currV.(margaret.BaseSeq)
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
			tipe:  margaret.BaseSeq(0),
			specs: []margaret.QuerySpec{margaret.Live(true)},
			live:  true,
			values: map[librarian.Addr][]interface{}{
				librarian.Addr([]byte{0, 0, 0, 2}):  []interface{}{2, 4, 6, 8, 10, 12, 14, 16, 18},
				librarian.Addr([]byte{0, 0, 0, 3}):  []interface{}{3, 6, 9, 12, 15, 18},
				librarian.Addr([]byte{0, 0, 0, 4}):  []interface{}{4, 8, 12, 16},
				librarian.Addr([]byte{0, 0, 0, 5}):  []interface{}{5, 10, 15},
				librarian.Addr([]byte{0, 0, 0, 6}):  []interface{}{6, 12, 18},
				librarian.Addr([]byte{0, 0, 0, 7}):  []interface{}{7, 14},
				librarian.Addr([]byte{0, 0, 0, 8}):  []interface{}{8, 16},
				librarian.Addr([]byte{0, 0, 0, 9}):  []interface{}{9, 18},
				librarian.Addr([]byte{0, 0, 0, 10}): []interface{}{10},
				librarian.Addr([]byte{0, 0, 0, 11}): []interface{}{11},
				librarian.Addr([]byte{0, 0, 0, 12}): []interface{}{12},
				librarian.Addr([]byte{0, 0, 0, 12}): []interface{}{12},
				librarian.Addr([]byte{0, 0, 0, 13}): []interface{}{13},
				librarian.Addr([]byte{0, 0, 0, 14}): []interface{}{14},
				librarian.Addr([]byte{0, 0, 0, 15}): []interface{}{15},
				librarian.Addr([]byte{0, 0, 0, 16}): []interface{}{16},
				librarian.Addr([]byte{0, 0, 0, 17}): []interface{}{17},
				librarian.Addr([]byte{0, 0, 0, 18}): []interface{}{18},
				librarian.Addr([]byte{0, 0, 0, 19}): []interface{}{19},
			},
		},
	}

	return func(t *testing.T) {
		for i, tc := range tcs {
			t.Run(fmt.Sprint(i), mkTest(tc))
		}
	}
}
