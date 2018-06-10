package test

import (
	"testing"

	"cryptoscope.co/go/librarian/test"
)

func TestBadger(t *testing.T) {
	t.Run("SetterIndex", test.RunSetterIndexTests)
	t.Run("SeqSetterIndex", test.RunSeqSetterIndexTests)
}
