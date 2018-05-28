package all

import (
	"testing"

	ltest "cryptoscope.co/go/librarian/test"

	// imported only for side effects / registring testing helpers
	_ "cryptoscope.co/go/librarian/badger"
	_ "cryptoscope.co/go/librarian/mapidx"
	_ "cryptoscope.co/go/margaret/mem"
	_ "cryptoscope.co/go/margaret/offset"
)

func Test(t *testing.T) {
	t.Run("SeqSetterIndex", ltest.RunSeqSetterIndexTests)
	t.Run("SetterIndex", ltest.RunSetterIndexTests)
	t.Run("SinkIndex", ltest.RunSinkIndexTests)
}
