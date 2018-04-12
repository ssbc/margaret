package remote

import (
	"testing"

	"cryptoscope.co/go/librarian"
	"cryptoscope.co/go/librarian/mapidx"
	"cryptoscope.co/go/librarian/test"
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
		Index: idx,
	}, nil
}
	

func TestRemote(t *testing.T) {
	t.Run("TestSetterIndex", test.TestSetterIndex(newIdx))
}
