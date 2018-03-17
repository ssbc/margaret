package test // import "cryptoscope.co/go/margaret/test"

import (
	"testing"

	"cryptoscope.co/go/margaret"
)

func LogTest(f func(name string, tipe interface{}) margaret.Log) func(*testing.T) {
	return func(t *testing.T) {
		t.Run("Get", LogTestGet(f))
		t.Run("Simple", LogTestSimple(f))
		t.Run("Concurrent", LogTestConcurrent(f))
	}
}
