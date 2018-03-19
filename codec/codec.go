package codec // import "cryptoscope.co/go/margaret/codec"

import (
	"io"
)

type Codec interface {
	// Marshal encodes a single value and returns the serialized byte slice.
	Marshal(value interface{}) ([]byte, error)

	// Unmarshal decodes and returns the value stored in data.
	Unmarshal(data []byte) (interface{}, error)

	NewDecoder(io.Reader) Decoder
	NewEncoder(io.Writer) Encoder
}

type Decoder interface {
	Decode() (interface{}, error)
}

type Encoder interface {
	Encode(v interface{}) error
}

// func (T interface{}) func(rw io.ReadWriter) Codec {...}
