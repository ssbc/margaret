package test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.cryptoscope.co/librarian"
	"go.cryptoscope.co/margaret"
)

type NewSeqSetterIndexFunc func(name string, tipe interface{}) (librarian.SeqSetterIndex, error)

func TestSeqSetterIndex(newIdx NewSeqSetterIndexFunc) func(*testing.T) {
	return func(t *testing.T) {
		t.Run("Sequential", TestSeqSetterIndexSequential(newIdx))
	}
}

func TestSeqSetterIndexSequential(newIdx NewSeqSetterIndexFunc) func(*testing.T) {
	return func(t *testing.T) {
		a := assert.New(t)
		r := require.New(t)
		ctx := context.Background()

		idx, err := newIdx(t.Name(), "str")
		r.NoError(err)
		r.NotNil(idx)

		seq, err := idx.GetSeq()
		a.NoError(err, "returned by GetSeq before setting")
		a.Equal(margaret.BaseSeq(-1), seq, "returned by GetSeq before setting")

		err = idx.Set(ctx, "test", "omg what is this")
		r.NoError(err, "error setting value")

		err = idx.SetSeq(margaret.BaseSeq(0))
		a.NoError(err, "returned by SetSeq")

		obv, err := idx.Get(ctx, "test")
		r.NoError(err, "error getting observable")
		r.NotNil(obv)

		seq, err = idx.GetSeq()
		a.NoError(err, "returned by GetSeq after setting")
		a.Equal(margaret.BaseSeq(0), seq, "returned by GetSeq after setting")

		v, err := obv.Value()
		a.NoError(err, "error getting value")
		a.Equal(v, "omg what is this")
	}
}
