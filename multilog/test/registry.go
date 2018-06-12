package test

import (
	"testing"
)

var NewLogFuncs map[string]NewLogFunc

func init() {
	NewLogFuncs = map[string]NewLogFunc{}
}

func Register(name string, f NewLogFunc) {
	NewLogFuncs[name] = f
}

func RunSinkTests(t *testing.T) {
	t.Logf("found multilogs %v", NewLogFuncs)
	for name, newLog := range NewLogFuncs {
		t.Run(name, SinkTest(newLog))
	}
}

func RunMultiLogTests(t *testing.T) {
	t.Logf("found multilogs %v", NewLogFuncs)
	for name, newLog := range NewLogFuncs {
		t.Run(name, MultiLogTest(newLog))
	}
}
