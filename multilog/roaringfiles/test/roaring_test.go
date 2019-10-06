// SPDX-License-Identifier: MIT

package test

import (
	"testing"

	"go.cryptoscope.co/margaret/multilog/test"
)

func TestRoaringFiles(t *testing.T) {
	t.Run("SubLog", test.RunSubLogTests)
	t.Run("MultiLog", test.RunMultiLogTests)
	t.Run("Sink", test.RunSinkTests)
}
