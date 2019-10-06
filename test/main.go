// SPDX-License-Identifier: MIT

package test // import "go.cryptoscope.co/margaret/test"

import (
	"testing"

	"go.cryptoscope.co/margaret"
)

type NewLogFunc func(string, interface{}) (margaret.Log, error)

func LogTest(f NewLogFunc) func(*testing.T) {
	return func(t *testing.T) {
		t.Run("Get", LogTestGet(f))
		t.Run("Simple", LogTestSimple(f))
		t.Run("Concurrent", LogTestConcurrent(f))
	}
}
