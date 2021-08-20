// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

package all

import (
	// imported only for side effects / registring testing helpers
	_ "go.cryptoscope.co/margaret/test/all"

	_ "go.cryptoscope.co/margaret/indexes/badger/test"
	_ "go.cryptoscope.co/margaret/indexes/mapidx/test"
	_ "go.cryptoscope.co/margaret/indexes/mkv/test"
)
