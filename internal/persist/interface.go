package persist

import (
	"errors"
	"io"
)

type Key []byte

var ErrNotFound = errors.New("persist: item not found")

type Saver interface {
	io.Closer
	Put(Key, []byte) error
	Get(Key) ([]byte, error)

	List() ([]Key, error)
}
