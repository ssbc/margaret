// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

package cbor

import (
	"bytes"
	"io"
	"reflect"

	"github.com/pkg/errors"
	ugorjiCodec "github.com/ugorji/go/codec"
	"go.cryptoscope.co/margaret"
)

// New creates a msgpack codec
// tipe is required because our Decode() interface doesn't take an argument
func New(tipe interface{}) margaret.Codec {
	ch := ugorjiCodec.CborHandle{}
	// ch.Canonical = true
	ch.StructToArray = true

	c := &codec{
		handle: &ch,
	}
	if tipe == nil {
		c.any = true
		return c
	}
	t := reflect.TypeOf(tipe)
	isPtr := t.Kind() == reflect.Ptr
	if isPtr {
		t = t.Elem()
	}
	c.tipe = t
	return c
}

type codec struct {
	tipe   reflect.Type
	any    bool
	handle *ugorjiCodec.CborHandle
}

func (c *codec) Marshal(v interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := c.NewEncoder(&buf)
	err := enc.Encode(v)
	return buf.Bytes(), errors.Wrap(err, "cbor codec: encode failed")
}

func (c *codec) Unmarshal(data []byte) (interface{}, error) {
	dec := c.NewDecoder(bytes.NewReader(data))
	return dec.Decode()
}

func (c *codec) NewEncoder(w io.Writer) margaret.Encoder {
	return ugorjiCodec.NewEncoder(w, c.handle)
}

func (c *codec) NewDecoder(r io.Reader) margaret.Decoder {
	dec := ugorjiCodec.NewDecoder(r, c.handle)
	return &decoder{tipe: c.tipe, dec: dec}
}

type decoder struct {
	tipe reflect.Type
	dec  *ugorjiCodec.Decoder
}

func (dec *decoder) Decode() (interface{}, error) {
	v := reflect.New(dec.tipe).Interface()
	err := dec.dec.Decode(&v)
	return reflect.ValueOf(v).Elem().Interface(), err
}
