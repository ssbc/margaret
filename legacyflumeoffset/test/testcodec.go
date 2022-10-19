// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

package test

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"

	"github.com/pkg/errors"
	"github.com/ssbc/margaret"
)

type testCodec struct{}

var _ margaret.Codec = (*testCodec)(nil)

func (c testCodec) Marshal(v interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := c.NewEncoder(&buf)
	err := enc.Encode(v)
	return buf.Bytes(), errors.Wrap(err, "cbor codec: encode failed")
}

func (c testCodec) Unmarshal(data []byte) (interface{}, error) {
	dec := c.NewDecoder(bytes.NewReader(data))
	return dec.Decode()
}

type testEncoder struct {
	w io.Writer
}

func (te testEncoder) Encode(v interface{}) error {
	return fmt.Errorf("writing not uspported")
}

func (c testCodec) NewEncoder(w io.Writer) margaret.Encoder {
	return testEncoder{w: w}
}

func (c testCodec) NewDecoder(r io.Reader) margaret.Decoder {
	return &decoder{r: r}
}

type decoder struct {
	r io.Reader
}

func (dec *decoder) Decode() (interface{}, error) {
	return ioutil.ReadAll(dec.r)
}
