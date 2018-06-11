package test

import (
	"testing"

	mtest "cryptoscope.co/go/margaret/test"
)

var NewLogFuncs map[string]NewLogFunc

func init() {
	NewLogFuncs = map[string]NewLogFunc{}
}

func Register(name string, f NewLogFunc) {
	NewLogFuncs[name] = f
}

func RunTests(t *testing.T) {
	t.Logf("found logs %v", mtest.NewLogFuncs)
	t.Logf("found multilogs %v", NewLogFuncs)
	for name, newLog := range NewLogFuncs {
		for mName, mNewLog := range mtest.NewLogFuncs {
			t.Run(name+"/"+mName, SinkTest(newLog, mNewLog))
		}
	}
}
