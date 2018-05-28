package test

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"cryptoscope.co/go/librarian"
	"cryptoscope.co/go/luigi"
	mtest "cryptoscope.co/go/margaret/test"
)

type NewSinkIndexFunc func(name string, tipe interface{}, f librarian.StreamProcFunc) (librarian.SinkIndex, error)

func TestSinkIndex(newLog mtest.NewLogFunc, newIdx NewSeqSetterIndexFunc) func(*testing.T) {
	return func(t *testing.T) {
		t.Run("WithBreak", TestSinkIndexWithBreak(newLog, newIdx))
	}
}

func TestSinkIndexWithBreak(newLog mtest.NewLogFunc, newIdx NewSeqSetterIndexFunc) func(*testing.T) {
	return func(t *testing.T) {
		a := assert.New(t)
		r := require.New(t)
		ctx := context.Background()

		f := func(ctx context.Context, v interface{}, idx librarian.SetterIndex) error {
			if strings.Contains(v.(string), "interesting") {
				return idx.Set(ctx, "interesting", v)
			} else if strings.Contains(v.(string), "boring") {
				return idx.Set(ctx, "boring", v)
			}

			return nil
		}

		seqSetIdx, err := newIdx(t.Name(), "str")
		r.NoError(err, "error creating SeqSetterIndex")

		idx := librarian.NewSinkIndex(f, seqSetIdx)
		log, err := newLog(t.Name(), "str")
		r.NoError(err, "error creating log")
		r.NotNil(log, "returned log is nil")

		defer func() {
			if namer, ok := log.(interface{ FileName() string }); ok {
				r.NoError(os.Remove(namer.FileName()), "error deleting log after test")
			}
		}()

		a.NoError(log.Append("boring string"), "error appending")
		a.NoError(log.Append("another boring string"), "error appending")
		a.NoError(log.Append("mildly interesting string"), "error appending")

		src, err := log.Query(idx.QuerySpec())
		a.NoError(err, "error querying log")
		a.NoError(luigi.Pump(ctx, idx, src), "error pumping from queried src to SinkIndex")

		obv, err := idx.Get(ctx, "interesting")
		r.NoError(err, "error getting interesting index")
		r.NotNil(obv, "returned no error but nil observable")
		v, err := obv.Value()
		a.NoError(err, "error getting interesting value from observable")
		a.Equal("mildly interesting string", v)

		obv, err = idx.Get(ctx, "boring")
		a.NoError(err, "error getting boring index")
		v, err = obv.Value()
		a.NoError(err, "error getting boring value from observable")
		a.Equal("another boring string", v)

		a.NoError(log.Append("so-so string"), "error appending")
		a.NoError(log.Append("highly interesting string"), "error appending")

		src, err = log.Query(idx.QuerySpec())
		a.NoError(err, "error querying log")
		a.NoError(luigi.Pump(ctx, idx, src), "error pumping from queried src to SinkIndex")

		obv, err = idx.Get(ctx, "interesting")
		a.NoError(err, "error getting interesting index")
		v, err = obv.Value()
		a.NoError(err, "error getting interesting value from observable")
		a.Equal("highly interesting string", v)

		obv, err = idx.Get(ctx, "boring")
		a.NoError(err, "error getting boring index")
		v, err = obv.Value()
		a.NoError(err, "error getting boring value from observable")
		a.Equal("another boring string", v)
	}
}
