// SPDX-License-Identifier: MIT

package test

import (
	"os"
	"strings"

	"github.com/pkg/errors"

	"go.cryptoscope.co/margaret"
	"go.cryptoscope.co/margaret/codec/json"
	"go.cryptoscope.co/margaret/codec/msgpack"
	"go.cryptoscope.co/margaret/framing/basic"
	"go.cryptoscope.co/margaret/framing/lengthprefixed"
	"go.cryptoscope.co/margaret/offset"
	mtest "go.cryptoscope.co/margaret/test"
)

var newLogFuncs map[string]mtest.NewLogFunc

func init() {
	newLogFuncs = make(map[string]mtest.NewLogFunc)

	codecs := map[string]mtest.NewCodecFunc{
		"json":    json.New,
		"msgpack": msgpack.New,
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
			newLogFuncs["offset/"+fname+"/"+cname] = buildNewLogFunc(frame, newCodec)
		}
	}
}
