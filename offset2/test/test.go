// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

package test

import (
	"github.com/ssbc/margaret"
	"github.com/ssbc/margaret/codec/cbor"
	"github.com/ssbc/margaret/codec/json"
	"github.com/ssbc/margaret/codec/msgpack"
	"github.com/ssbc/margaret/offset2"
	mtest "github.com/ssbc/margaret/test"
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
