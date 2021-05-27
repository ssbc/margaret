// SPDX-License-Identifier: MIT

package all

import (
	"testing"

	ltest "go.cryptoscope.co/margaret/indexes/test"
)

func Test(t *testing.T) {
	t.Run("SeqSetterIndex", ltest.RunSeqSetterIndexTests)
	t.Run("SetterIndex", ltest.RunSetterIndexTests)
	t.Run("SinkIndex", ltest.RunSinkIndexTests)
}
