// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

package test

import (
	"go.cryptoscope.co/margaret"
	"go.cryptoscope.co/margaret/codec/cbor"
	"go.cryptoscope.co/margaret/codec/json"
	"go.cryptoscope.co/margaret/codec/msgpack"
	"go.cryptoscope.co/margaret/offset2"
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
			// name = strings.Replace(name, "/", "_", -1)
			return offset2.Open(name, newCodec(tipe))
		}
	}

	for cname, newCodec := range codecs {
		mtest.Register("offset2/"+cname, buildNewLogFunc(newCodec))
		newLogFuncs["offset2/"+cname] = buildNewLogFunc(newCodec)
	}
}
