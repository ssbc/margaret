// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

package test

import (
	"github.com/ssbc/margaret/indexes"
	"github.com/ssbc/margaret/indexes/mapidx"
	"github.com/ssbc/margaret/indexes/test"
)

func init() {
	newSeqSetterIdx := func(name string, tipe interface{}) (indexes.SeqSetterIndex, error) {
		return mapidx.New(), nil
	}

	newSetterIdx := func(name string, tipe interface{}) (indexes.SetterIndex, error) {
		return mapidx.New(), nil
	}

	test.RegisterSeqSetterIndex("mapidx", newSeqSetterIdx)
	test.RegisterSetterIndex("mapidx", newSetterIdx)
}
