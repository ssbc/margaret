// SPDX-License-Identifier: MIT

package test

import (
	"os"
	"path/filepath"

	"go.cryptoscope.co/margaret"
	"go.cryptoscope.co/margaret/codec/cbor"
	"go.cryptoscope.co/margaret/codec/json"
	"go.cryptoscope.co/margaret/codec/msgpack"
	lfo "go.cryptoscope.co/margaret/legacyflumeoffset"
	mtest "go.cryptoscope.co/margaret/test"
)

var newLogFuncs map[string]mtest.NewLogFunc

func init() {
	newLogFuncs = make(map[string]mtest.NewLogFunc)

	codecs := map[string]mtest.NewCodecFunc{
		"json":    json.New,
		"msgpack": msgpack.New,
		"cbor":    cbor.New,
	}

	buildNewLogFunc := func(newCodec mtest.NewCodecFunc) mtest.NewLogFunc {
		return func(name string, tipe interface{}) (margaret.Log, error) {
			path := filepath.Join("testrun", name)
			os.RemoveAll(path)
			return lfo.Open(path, newCodec(tipe))
		}
	}

	for cname, newCodec := range codecs {
		newLogFuncs["lfo/"+cname] = buildNewLogFunc(newCodec)
	}
}
