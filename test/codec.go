package test

import (
	"cryptoscope.co/go/margaret"
)

// NewCodecFunc is a function that returns a codec
type NewCodecFunc func(tipe interface{}) margaret.Codec
