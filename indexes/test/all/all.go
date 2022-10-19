// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

package all

import (
	// imported only for side effects / registring testing helpers
	_ "github.com/ssbc/margaret/test/all"

	_ "github.com/ssbc/margaret/indexes/badger/test"
	_ "github.com/ssbc/margaret/indexes/mapidx/test"
	_ "github.com/ssbc/margaret/indexes/mkv/test"
)
