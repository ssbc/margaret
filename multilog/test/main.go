package test // import "cryptoscope.co/go/margaret/multilog/test"

import (
	"testing"

	"cryptoscope.co/go/margaret/multilog"
	mtest "cryptoscope.co/go/margaret/test"
)

type NewLogFunc func(name string, tipe interface{}) (multilog.MultiLog, error)

func SinkTest(f NewLogFunc, g mtest.NewLogFunc) func(*testing.T) {
	return func(t *testing.T) {
		t.Run("Simple", SinkTestSimple(f, g))
	}
}
