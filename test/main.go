package test // import "cryptoscope.co/go/margaret/test"

import (
	"testing"

	"cryptoscope.co/go/margaret"
)

type NewLogFunc func(string, interface{}) (margaret.Log, error)

func LogTest(f NewLogFunc) func(*testing.T) {
	return func(t *testing.T) {
		t.Run("Get", LogTestGet(f))
		t.Run("Simple", LogTestSimple(f))
		t.Run("Concurrent", LogTestConcurrent(f))
	}
}
