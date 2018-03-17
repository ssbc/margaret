package json // import "cryptoscope.co/go/margaret/codec/json"

import (
  "encoding/json"
  "io"
  "reflect"

  cdc "cryptoscope.co/go/margaret/codec"
)

// NewCodec creates a json codec that decodes into values of type tipe.
func NewCodec(tipe interface{}) cdc.Codec {
  if tipe == nil {
    return &codec{any: true}
  }

  t := reflect.TypeOf(tipe)
  isPtr := t.Kind() == reflect.Ptr
  if isPtr {
    t = t.Elem()
  }

  return &codec{
    tipe: t,
    asPtr: isPtr,
  }
}

type codec struct{
  tipe reflect.Type
  asPtr bool
  any bool
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

func (*codec) NewEncoder(w io.Writer) cdc.Encoder {
  return json.NewEncoder(w)
}

func (c *codec) NewDecoder(r io.Reader) cdc.Decoder {
  return &decoder{
    tipe: c.tipe,
    dec: json.NewDecoder(r),
  }
}

type decoder struct {
  tipe reflect.Type
  dec *json.Decoder
}

func (dec *decoder) Decode() (interface{}, error) {
  v := reflect.New(dec.tipe).Interface()
  err := dec.dec.Decode(v)

  return v, err
}
