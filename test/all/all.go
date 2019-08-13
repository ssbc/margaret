// SPDX-License-Identifier: MIT

package all

import (
	// import to register testing helpers
	_ "go.cryptoscope.co/margaret/mem/test"

	// offset1 tests pass minus the reverse ones. please update to v2
	// _ "go.cryptoscope.co/margaret/offset/test"

	_ "go.cryptoscope.co/margaret/offset2/test"

	// SQLite is work in progress (mostly unclear how to support live querys.. observables?!)
	_ "go.cryptoscope.co/margaret/sqlite/test"
)
