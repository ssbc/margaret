// SPDX-License-Identifier: MIT

package test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.cryptoscope.co/luigi"
	librarian "go.cryptoscope.co/margaret/indexes"
)

type NewSetterIndexFunc func(name string, tipe interface{}) (librarian.SetterIndex, error)

func TestSetterIndex(newIdx NewSetterIndexFunc) func(*testing.T) {
	return func(t *testing.T) {
		t.Run("Sequential", TestSetterIndexSequential(newIdx))
		t.Run("Observable", TestSetterIndexObservable(newIdx))
	}
}

func TestSetterIndexSequential(newIdx NewSetterIndexFunc) func(*testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()
		r := require.New(t)

		idx, err := newIdx(t.Name(), "str")
		r.NoError(err, "error creating index")

		err = idx.Set(ctx, "test", "omg what is this")
		r.NoError(err, "error setting value")

		obv, err := idx.Get(ctx, "test")
		r.NoError(err, "error getting observable")

		v, err := obv.Value()
		r.NoError(err, "error getting value")

		if v != "omg what is this" {
			t.Errorf("expected %q but got %q (type: %T)", "omg what is this", v, v)
		}
	}
}

func TestSetterIndexObservable(newIdx NewSetterIndexFunc) func(*testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()
		r := require.New(t)

		idx, err := newIdx(t.Name(), "str")
		r.NoError(err, "error creating index")

		obv, err := idx.Get(ctx, "test")
		r.NoError(err, "error getting observable")

		var i int
		first := make(chan struct{})
		closed := make(chan struct{})
		rxExp := []interface{}{
			librarian.UnsetValue{"test"},
			"omg what is this",
			"so rad",
			"wowzers",
			librarian.UnsetValue{"test"},
		}

		var cancel func()
		cancel = obv.Register(luigi.FuncSink(func(ctx context.Context, v interface{}, err error) error {
			if i == 0 {
				close(first)
			}

			if i == len(rxExp)-1 {
				t.Log("got all messages, canceling registration")
				defer cancel()
			}

			defer func() { i++ }()

			if err != nil {
				if err != (luigi.EOS{}) {
					t.Log("sink closed with non-EOS error:", err)
				}

				if i == len(rxExp) {
					close(closed)
				} else {
					t.Errorf("unexpected close: i=%d", i)
				}

				return nil
			}

			if i > len(rxExp)-1 {
				return nil
			}

			if v != rxExp[i] {
				t.Errorf("expecting %q, but got %q", rxExp[i], v)
			}

			return nil
		}))

		<-first

		exp := []string{
			"omg what is this",
			"so rad",
			"wowzers",
		}

		for _, v := range exp {
			err = idx.Set(ctx, "test", v)
			if err != nil {
				t.Errorf("error setting value to %q: %s", v, err)
			}
		}

		err = idx.Delete(ctx, "test")
		r.NoError(err, "error deleting value")

		<-closed
	}
}
