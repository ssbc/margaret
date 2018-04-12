package mapidx // import "cryptoscope.co/go/librarian/mapidx"

import (
	"testing"

	"cryptoscope.co/go/librarian"
	"cryptoscope.co/go/librarian/test"
)

func newIdx(name string, tipe interface{}) (librarian.SetterIndex, error) {
	return New(), nil
}

func TestMap(t *testing.T) {
	t.Run("TestSetterIndex", test.TestSetterIndex(newIdx))
}

