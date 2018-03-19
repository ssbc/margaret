package offset // import "cryptoscope.co/go/margaret/offset"

import (
	"os"
	"strings"
	"testing"

	"cryptoscope.co/go/margaret"
	"cryptoscope.co/go/margaret/codec/json"
	basic "cryptoscope.co/go/margaret/framing/basic"
	lenpref "cryptoscope.co/go/margaret/framing/lengthprefixed"
	"cryptoscope.co/go/margaret/test"
)

type testStruct struct{}

func JSONOffsetLog(framing Framing) func(*testing.T) {
	return func(t *testing.T) {
		f := func(name string, tipe interface{}) margaret.Log {
			name = strings.Replace(name, "/", "_", -1)
			f, err := os.Create(name)
			if err != nil {
				t.Fatal("error opening offset file", err)
			}

			return NewOffsetLog(f, framing, json.NewCodec(tipe))
		}
		t.Run("JSON", test.LogTest(f))
	}
}

func TestOffsetLog(t *testing.T) {
	framings := map[string]Framing{
		"lengthprefixed": lenpref.New32(defaultFrameSize),
		"basic":          basic.New32(defaultFrameSize),
	}

	for name, framing := range framings {
		t.Run(name, JSONOffsetLog(framing))
	}
}
