package testall

import (
	"testing"

	_ "cryptoscope.co/go/margaret/mem"
	_ "cryptoscope.co/go/margaret/offset"

	mtest "cryptoscope.co/go/margaret/test"
)

func TestLog(t *testing.T) {
	mtest.RunTests(t)
}
