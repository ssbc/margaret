package test

import (
	"os"
	"strings"

	"github.com/pkg/errors"

	"cryptoscope.co/go/margaret"
	"cryptoscope.co/go/margaret/codec/json"
	"cryptoscope.co/go/margaret/framing/basic"
	"cryptoscope.co/go/margaret/framing/lengthprefixed"
	"cryptoscope.co/go/margaret/offset"
	mtest "cryptoscope.co/go/margaret/test"
)

func init() {
	codecs := map[string]mtest.NewCodecFunc{
		"json": json.New,
	}

	framings := map[string]margaret.Framing{
		"basic":          basic.New32(offset.DefaultFrameSize),
		"lengthprefixed": lengthprefixed.New32(offset.DefaultFrameSize),
	}

	buildNewLogFunc := func(framing margaret.Framing, newCodec mtest.NewCodecFunc) mtest.NewLogFunc {
		return func(name string, tipe interface{}) (margaret.Log, error) {
			name = strings.Replace(name, "/", "_", -1)
			f, err := os.Create(name)
			if err != nil {
				return nil, errors.Wrap(err, "error creating database file")
			}

			return offset.New(f, framing, newCodec(tipe))
		}
	}

	for cname, newCodec := range codecs {
		for fname, frame := range framings {
			mtest.Register("offset/"+fname+"/"+cname, buildNewLogFunc(frame, newCodec))
		}
	}
}
