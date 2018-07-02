package test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
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
		ctx := context.Background()

		idx, err := newIdx(t.Name(), "str")
		if err != nil {
			t.Error("error creating index", err)
		}

		seq, err := idx.GetSeq()
		a.NoError(err, "returned by GetSeq before setting")
		a.Equal(margaret.BaseSeq(-1), seq, "returned by GetSeq before setting")

		err = idx.Set(ctx, "test", "omg what is this")
		if err != nil {
			t.Error("error setting value", err)
		}

		err = idx.SetSeq(margaret.BaseSeq(0))
		a.NoError(err, "returned by SetSeq")

		obv, err := idx.Get(ctx, "test")
		if err != nil {
			t.Error("error getting observable", err)
		}

		seq, err = idx.GetSeq()
		a.NoError(err, "returned by GetSeq after setting")
		a.Equal(margaret.BaseSeq(0), seq, "returned by GetSeq after setting")

		v, err := obv.Value()
		if err != nil {
			t.Error("error getting value", err)
		}
		if v != "omg what is this" {
			t.Errorf("expected %q but got %q (type: %T)", "omg what is this", v, v)
		}
	}
}
