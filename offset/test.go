package offset

import (
	"os"
	"strings"

	"github.com/pkg/errors"

	"cryptoscope.co/go/margaret"
	"cryptoscope.co/go/margaret/codec/json"
	"cryptoscope.co/go/margaret/framing/basic"
	"cryptoscope.co/go/margaret/framing/lengthprefixed"
	mtest "cryptoscope.co/go/margaret/test"
)

func init() {
	codecs := map[string]margaret.NewCodecFunc{
		"json": json.New,
	}

	framings := map[string]margaret.Framing{
		"basic":          basic.New32(DefaultFrameSize),
		"lengthprefixed": lengthprefixed.New32(DefaultFrameSize),
	}

	buildNewLogFunc := func(framing margaret.Framing, newCodec margaret.NewCodecFunc) mtest.NewLogFunc {
		return func(name string, tipe interface{}) (margaret.Log, error) {
			name = strings.Replace(name, "/", "_", -1)
			f, err := os.Create(name)
			if err != nil {
				return nil, errors.Wrap(err, "error creating database file")
			}

			return NewOffsetLog(f, framing, newCodec(tipe)), nil
		}
	}

	for cname, newCodec := range codecs {
		for fname, frame := range framings {
			mtest.Register("offset/"+fname+"/"+cname, buildNewLogFunc(frame, newCodec))
		}
	}
}
