package test

import (
	"go.cryptoscope.co/margaret"
	"go.cryptoscope.co/margaret/codec/cbor"
	"go.cryptoscope.co/margaret/codec/json"
	"go.cryptoscope.co/margaret/codec/msgpack"
	"go.cryptoscope.co/margaret/sqlite"
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
			log, err := sqlite.Open(name, newCodec(tipe))
			if err != nil {
				return nil, err
			}
			db := log.DB()
			_, err = db.Exec(`delete from margaret_log`)
			return log, err
		}
	}

	for cname, newCodec := range codecs {
		mtest.Register("sqlite/"+cname, buildNewLogFunc(newCodec))
		newLogFuncs["sqlite/"+cname] = buildNewLogFunc(newCodec)
	}
}
