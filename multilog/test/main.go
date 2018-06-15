package test // import "go.cryptoscope.co/margaret/multilog/test"

import (
	"testing"

	"go.cryptoscope.co/margaret/multilog"
)

type NewLogFunc func(name string, tipe interface{}) (multilog.MultiLog, error)

func SinkTest(f NewLogFunc) func(*testing.T) {
	return func(t *testing.T) {
		t.Run("Simple", SinkTestSimple(f))
	}
}

func MultiLogTest(f NewLogFunc) func(*testing.T) {
	return func(t *testing.T) {
		t.Run("Simple", MultiLogTestSimple(f))
	}
}

func SubLogTest(f NewLogFunc) func(*testing.T) {
	return func(t *testing.T) {
		t.Run("Get", SubLogTestGet(f))
	}
}
