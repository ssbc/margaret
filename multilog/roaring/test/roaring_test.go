// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

package test

import (
	"testing"

	"github.com/ssbc/margaret/multilog/test"
)

func TestRoaringFiles(t *testing.T) {
	t.Run("SubLog", test.RunSubLogTests)
	t.Run("MultiLog", test.RunMultiLogTests)
	t.Run("Sink", test.RunSinkTests)
}
