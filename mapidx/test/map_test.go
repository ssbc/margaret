package test

import (
	"testing"

	"go.cryptoscope.co/librarian/test"
)

func TestMap(t *testing.T) {
	t.Run("SetterIndex", test.RunSetterIndexTests)
	t.Run("SeqSetterIndex", test.RunSeqSetterIndexTests)
}
