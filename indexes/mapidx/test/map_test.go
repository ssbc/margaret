// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

package test

import (
	"testing"

	"go.cryptoscope.co/margaret/indexes/test"
)

func TestMap(t *testing.T) {
	t.Run("SetterIndex", test.RunSetterIndexTests)
	t.Run("SeqSetterIndex", test.RunSeqSetterIndexTests)
}
