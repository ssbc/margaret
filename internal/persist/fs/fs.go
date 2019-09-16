package fs

import (
	"fmt"
	"os"

	"github.com/pkg/errors"
	"go.cryptoscope.co/margaret/internal/persist"
)

type Saver struct {
	base string
}

var _ persist.Saver = (*Saver)(nil)

func New(base string) *Saver {
	return &Saver{base: base}
}

func (s Saver) Put(key persist.Key, data []byte) error {
	return fmt.Errorf("TODO")
}

func (s Saver) Get(key persist.Key) ([]byte, error) {
	return []byte("TODO:data"), os.ErrNotExist
}

func (s Saver) List() ([]persist.Key, error) {
	return nil, errors.Wrap(os.ErrNotExist, "TODO:list")
}
