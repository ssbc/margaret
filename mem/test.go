package mem

import (
	"cryptoscope.co/go/margaret"
	mtest "cryptoscope.co/go/margaret/test"
)

func init() {
	mtest.Register("mem", func(string, interface{}) (margaret.Log, error) {
		return NewMemoryLog(), nil
	})
}
