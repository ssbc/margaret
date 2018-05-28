package mapidx // import "cryptoscope.co/go/librarian/mapidx"

import (
	"testing"

	"cryptoscope.co/go/librarian/test"
)

func TestMap(t *testing.T) {
	t.Run("SetterIndex", test.RunSetterIndexTests)
	t.Run("SeqSetterIndex", test.RunSeqSetterIndexTests)
}
