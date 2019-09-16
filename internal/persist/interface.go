package persist

import "errors"

type Key []byte

var ErrNotFound = errors.New("persist: item not found")

type Saver interface {
	Put(Key, []byte) error
	Get(Key) ([]byte, error)

	List() ([]Key, error)
}
