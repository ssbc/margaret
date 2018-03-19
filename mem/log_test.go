package mem // import "cryptoscope.co/go/margaret/mem"

import (
	"testing"

	"cryptoscope.co/go/margaret"
	"cryptoscope.co/go/margaret/test"
)

func TestMemoryLog(t *testing.T) {
	t.Run("Memlog", test.LogTest(
		func(string, interface{}) margaret.Log {
			return NewMemoryLog()
		}))
}
