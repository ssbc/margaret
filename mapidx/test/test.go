package test

import (
	"cryptoscope.co/go/librarian"
	"cryptoscope.co/go/librarian/mapidx"
	"cryptoscope.co/go/librarian/test"
)

func init() {
	newSeqSetterIdx := func(name string, tipe interface{}) (librarian.SeqSetterIndex, error) {
		return mapidx.New(), nil
	}

	newSetterIdx := func(name string, tipe interface{}) (librarian.SetterIndex, error) {
		return mapidx.New(), nil
	}

	test.RegisterSeqSetterIndex("mapidx", newSeqSetterIdx)
	test.RegisterSetterIndex("mapidx", newSetterIdx)
}
