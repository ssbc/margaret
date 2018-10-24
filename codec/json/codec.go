package json // import "go.cryptoscope.co/margaret/codec/json"

import (
	"encoding/json"
	"io"
	"reflect"

	"go.cryptoscope.co/margaret"
)

// New creates a json codec that decodes into values of type tipe.
func New(tipe interface{}) margaret.Codec {
	if tipe == nil {
		return &codec{any: true}
	}

	t := reflect.TypeOf(tipe)
	isPtr := t.Kind() == reflect.Ptr
	if isPtr {
		t = t.Elem()
	}

	return &codec{
		tipe:  t,
		asPtr: isPtr,
	}
}

type codec struct {
	tipe  reflect.Type
	asPtr bool
	any   bool
}

func (*codec) Marshal(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func (c *codec) Unmarshal(data []byte) (interface{}, error) {
	var v interface{}
	if !c.any {
		v = reflect.New(c.tipe).Interface()
	}

	err := json.Unmarshal(data, v)

	if !c.asPtr {
		v = reflect.ValueOf(v).Elem().Interface()
	}

	return v, err
}

func (*codec) NewEncoder(w io.Writer) margaret.Encoder {
	return json.NewEncoder(w)
}

func (c *codec) NewDecoder(r io.Reader) margaret.Decoder {
	return &decoder{
		tipe:  c.tipe,
		dec:   json.NewDecoder(r),
		asPtr: c.asPtr,
	}
}

type decoder struct {
	tipe  reflect.Type
	dec   *json.Decoder
	asPtr bool
}

func (dec *decoder) Decode() (interface{}, error) {
	v := reflect.New(dec.tipe).Interface()
	err := dec.dec.Decode(v)

	if !dec.asPtr {
		v = reflect.ValueOf(v).Elem().Interface()
	}

	return v, err
}
