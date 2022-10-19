// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

package test

import (
	"context"
	"testing"
	"time"

	"github.com/pkg/errors"

	"github.com/ssbc/go-luigi"
	"github.com/ssbc/margaret"
	"github.com/ssbc/margaret/indexes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func MultilogLiveQueryCheck(f NewLogFunc) func(*testing.T) {
	return func(t *testing.T) {
		a := assert.New(t)
		r := require.New(t)
		ctx, cancel := context.WithCancel(context.TODO())

		mlog, _, err := f(t.Name(), int64(0), "")
		r.NoError(err)

		// empty yet
		addrs, err := mlog.List()
		r.NoError(err, "error listing mlog")
		r.Len(addrs, 0)

		testLogs := map[indexes.Addr][]int64{
			"fii": {1, 2, 3},
			"faa": {100, 200, 300},
			"foo": {4, 5, 6},
			"fum": {7, 8, 9},
		}

		// fill in some values
		for name, vals := range testLogs {
			slog, err := mlog.Get(name)
			r.NoError(err)

			for i, v := range vals {
				_, err := slog.Append(v)
				r.NoError(err, "valied to append %s:%d", name, i)
			}

			r.EqualValues(slog.Seq(), len(vals)-1)
		}

		logOfFaa, err := mlog.Get(indexes.Addr("faa"))
		r.NoError(err)

		// produce new values in the background
		go func() {
			time.Sleep(time.Second / 10)
			slog, err := mlog.Get(indexes.Addr("faa"))
			if err != nil {
				panic(err)
			}
			for tv := 400; tv < 2000; tv += 100 {
				appendedSeq, err := slog.Append(tv)
				if err != nil {
					panic(err)
				}
				t.Log(tv, " inserted as:", appendedSeq)
				// !!!! handbrake to reduce chan send shedule madness
				time.Sleep(time.Second / 10)
				// !!!!!
			}
			time.Sleep(time.Second / 2)
			cancel()
		}()

		seqSrc, err := logOfFaa.Query(
			margaret.Gt(2),
			margaret.Live(true),
			margaret.SeqWrap(true),
		)
		r.NoError(err)

		var expSeq = 3
		var expVal = 400
		for {
			swv, err := seqSrc.Next(ctx)
			if err != nil {
				if luigi.IsEOS(err) || errors.Is(err, context.Canceled) {
					t.Log("canceled", err, swv)
					a.Equal(expSeq, 19)
					break
				}
				r.NoError(err)
			}
			sw := swv.(margaret.SeqWrapper)

			gotVal := sw.Value().(int64)

			a.EqualValues(expVal, gotVal, "wrong actual val")
			expVal += 100

			a.EqualValues(expSeq, sw.Seq(), "wrong seq value from query")
			t.Log(expSeq, sw.Seq())
			expSeq++
		}

		r.NoError(mlog.Close())
	}
}
