package mem // import "cryptoscope.co/go/margaret/mem"

import (
	"testing"

	"cryptoscope.co/go/margaret/test"
)

func TestMemoryLog(t *testing.T) {
	t.Run("Memlog", test.LogTest(NewMemoryLog))
}
