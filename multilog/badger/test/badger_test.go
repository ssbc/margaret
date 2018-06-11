package test

import (
	"testing"

	"cryptoscope.co/go/margaret/multilog/test"
)

func TestBadger(t *testing.T) {
	t.Run("Sink", test.RunTests)
}
