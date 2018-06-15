package test

import (
	"testing"

	"go.cryptoscope.co/librarian/test"
)

func TestBadger(t *testing.T) {
	t.Run("SetterIndex", test.RunSetterIndexTests)
	t.Run("SeqSetterIndex", test.RunSeqSetterIndexTests)
}
