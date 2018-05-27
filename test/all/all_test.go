package testall

import (
	"testing"

	_ "cryptoscope.co/go/margaret/mem"
	_ "cryptoscope.co/go/margaret/offset"

	mtest "cryptoscope.co/go/margaret/test"
)

func TestLog(t *testing.T) {
	for name, newLog := range mtest.NewLogFuncs {
		t.Run(name, mtest.LogTest(newLog))
	}
}
