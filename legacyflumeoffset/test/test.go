// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

package test

import (
	"os"
	"path/filepath"

	"github.com/ssbc/margaret"
	"github.com/ssbc/margaret/codec/cbor"
	"github.com/ssbc/margaret/codec/json"
	"github.com/ssbc/margaret/codec/msgpack"
	lfo "github.com/ssbc/margaret/legacyflumeoffset"
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
			path := filepath.Join("testrun", name)
			os.RemoveAll(path)
			return lfo.Open(path, newCodec(tipe))
		}
	}

	for cname, newCodec := range codecs {
		newLogFuncs["lfo/"+cname] = buildNewLogFunc(newCodec)
	}
}
