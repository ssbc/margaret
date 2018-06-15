package remote

import (
	"testing"

	"go.cryptoscope.co/librarian"
	"go.cryptoscope.co/librarian/mapidx"
	"go.cryptoscope.co/librarian/test"
)

type setterindex struct {
	librarian.Setter
	librarian.Index
}

func newIdx(name string, tipe interface{}) (librarian.SetterIndex, error) {
	idx := mapidx.New()
	sink := NewSink(idx)
	setter := NewSetter(sink)

	return setterindex{
		Setter: setter,
		Index:  idx,
	}, nil
}

func TestRemote(t *testing.T) {
	t.Run("TestSetterIndex", test.TestSetterIndex(newIdx))
}
