package basic // import "cryptoscope.co/go/margaret/framing/basic"

import (
	"encoding/binary"

	"cryptoscope.co/go/margaret"

	"github.com/pkg/errors"
)

type Framing interface {
	margaret.Framing

	FrameSize() int64
}

var _ Framing = &frame32{}

// New32 returns a new framing for blocks of size framesize.
// It prefixes the block by the data's length in 32bit big endian format.
func New32(framesize int) Framing {
	return &frame32{framesize}
}

type frame32 struct {
	framesize int
}

func (f *frame32) DecodeFrame(block []byte) ([]byte, error) {
	if len(block) != f.framesize {
		return nil, errors.New("wrong block size")
	}

	sizeStart := int(binary.BigEndian.Uint32(block[:4]))
	if sizeStart+8 > f.framesize {
		return nil, errors.New("frame size too large")
	}

	sizeEnd := int(binary.BigEndian.Uint32(block[sizeStart+4 : sizeStart+8]))
	if sizeStart != sizeEnd {
		return nil, errors.New("frame sizes don't match")
	}
	return block[4 : sizeStart+4], nil

}

func (f *frame32) EncodeFrame(data []byte) ([]byte, error) {
	if len(data)+8 > f.framesize {
		return nil, errors.New("data too long")
	}

	frame := make([]byte, f.framesize)
	binary.BigEndian.PutUint32(frame[:4], uint32(len(data)))
	binary.BigEndian.PutUint32(frame[len(data)+4:len(data)+8], uint32(len(data)))
	copy(frame[4:], data)

	return frame, nil
}

func (f *frame32) FrameSize() int64 {
	return int64(f.framesize)
}
