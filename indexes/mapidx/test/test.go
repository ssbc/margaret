// SPDX-License-Identifier: MIT

package test

import (
	"go.cryptoscope.co/margaret/indexes"
	"go.cryptoscope.co/margaret/indexes/mapidx"
	"go.cryptoscope.co/margaret/indexes/test"
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
