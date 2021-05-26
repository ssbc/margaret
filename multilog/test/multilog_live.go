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

		// produce new values in the background
		go func() {
			time.Sleep(time.Second / 10)
			slog, err := mlog.Get(librarian.Addr("faa"))
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
			margaret.Gt(margaret.BaseSeq(2)),
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

			gotVal := sw.Value().(margaret.BaseSeq)

			a.EqualValues(expVal, gotVal, "wrong actual val")
			expVal += 100
			seq := sw.Seq().(margaret.Seq)

			a.EqualValues(expSeq, seq.Seq(), "wrong seq value from query")
			t.Log(expSeq, seq.Seq())
			expSeq++
		}

		r.NoError(mlog.Close())
	}
}
