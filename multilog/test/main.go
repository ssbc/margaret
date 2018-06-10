package test // import "cryptoscope.co/go/margaret/test"

import (
	"testing"

	"cryptoscope.co/go/margaret/multilog"
)

type NewLogFunc func(name string, type interface{}) (multilog.MultiLog, error)

func LogTest(f NewLogFunc) func(*testing.T) {
	return func(t *testing.T) {
		t.Run("Simple", LogTestSimple(f))
	}
}
