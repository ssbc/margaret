package test

import (
	"testing"

	"cryptoscope.co/go/margaret/multilog/test"
)

func TestBadger(t *testing.T) {
	t.Run("SubLog", test.RunSubLogTests)
	t.Run("MultiLog", test.RunMultiLogTests)
	t.Run("Sink", test.RunSinkTests)
}
