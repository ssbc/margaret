package all

import (
	"testing"

	mltest "cryptoscope.co/go/margaret/multilog/test"
)

func TestSink(t *testing.T) {
	mltest.RunSinkTests(t)
}

func TestMultiLog(t *testing.T) {
	mltest.RunMultiLogTests(t)
}
