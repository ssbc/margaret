package test

import (
	"cryptoscope.co/go/margaret"
	"cryptoscope.co/go/margaret/mem"
	mtest "cryptoscope.co/go/margaret/test"
)

func init() {
	mtest.Register("mem", func(string, interface{}) (margaret.Log, error) {
		return mem.NewMemoryLog(), nil
	})
}
