package test

import (
	"context"
	"testing"
	"time"

	"github.com/pkg/errors"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.cryptoscope.co/librarian"
	"go.cryptoscope.co/luigi"
	"go.cryptoscope.co/margaret"
)

func MultilogLiveQueryCheck(f NewLogFunc) func(*testing.T) {
	return func(t *testing.T) {
		a := assert.New(t)
		r := require.New(t)
		ctx, cancel := context.WithCancel(context.TODO())

		mlog, _, err := f(t.Name(), margaret.BaseSeq(0), "")
		r.NoError(err)

		// empty yet
		addrs, err := mlog.List()
		r.NoError(err, "error listing mlog")
		r.Len(addrs, 0)

		testLogs := map[librarian.Addr][]margaret.BaseSeq{
			"fii": []margaret.BaseSeq{1, 2, 3},
			"faa": []margaret.BaseSeq{100, 200, 300},
			"foo": []margaret.BaseSeq{4, 5, 6},
			"fum": []margaret.BaseSeq{7, 8, 9},
		}

		// fill in some values
		for name, vals := range testLogs {
			slog, err := mlog.Get(name)
			r.NoError(err)

			for i, v := range vals {
				_, err := slog.Append(v)
				r.NoError(err, "valied to append %s:%d", name, i)
			}

			v, err := slog.Seq().Value()
			r.NoError(err)
			r.EqualValues(v, len(vals)-1)
		}

		logOfFaa, err := mlog.Get(librarian.Addr("faa"))
		r.NoError(err)

		seqSrc, err := logOfFaa.Query(
			margaret.Gt(margaret.BaseSeq(2)),
			margaret.Live(true),
		)
		r.NoError(err)

		// produce new values in the background
		go func() {
			time.Sleep(1 * time.Second)
			slog, err := mlog.Get(librarian.Addr("faa"))
			if err != nil {
				panic(err)
			}
			for tv := 400; tv < 1000; tv += 100 {
				seq, err := slog.Append(tv)
				if err != nil {
					panic(err)
				}
				t.Log(tv, " inserted as:", seq)
				// !!!!!
				time.Sleep(time.Second / 10)
				// !!!!!
			}
			time.Sleep(1 * time.Second)
			cancel()
		}()

		var expSeq = 3
		for {
			seqV, err := seqSrc.Next(ctx)
			if err != nil {
				if luigi.IsEOS(err) || errors.Cause(err) == context.Canceled {
					t.Log("canceled")
					a.Equal(expSeq, 9)
					break
				}
				r.NoError(err)
			}
			seq := seqV.(margaret.Seq)
			a.EqualValues(expSeq, seq.Seq(), "wrong seq value from query")
			expSeq++
		}
	}
}
