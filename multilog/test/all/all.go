package all

import (
	// import to register testing helpers
	_ "go.cryptoscope.co/margaret/mem/test"
	_ "go.cryptoscope.co/margaret/offset/test"

	_ "go.cryptoscope.co/margaret/multilog/badger/test"
)
