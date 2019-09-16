package test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.cryptoscope.co/margaret/internal/persist"
	"go.cryptoscope.co/margaret/internal/persist/fs"
)

func SimpleSaver(p persist.Saver) func(*testing.T) {

	return func(t *testing.T) {
		r := require.New(t)

		l, err := p.List()
		r.NoError(err)
		r.Len(l, 0)

		k := persist.Key{0, 0, 0, 1}
		d, err := p.Get(k)
		r.EqualError(err, persist.ErrNotFound.Error())
		r.Nil(d)

		err = p.Put(k, []byte("fooo"))
		r.NoError(err)

		l, err = p.List()
		r.NoError(err)
		r.Len(l, 0)
		r.Equal(k, l[0])
	}
}

func TestSaver(t *testing.T) {
	t.Run("Simple", SimpleSaver(makeFS(t)))
}

func makeFS(t *testing.T) persist.Saver {
	base := filepath.Join("testrun", t.Name())
	os.RemoveAll(base)
	return fs.New(base)
}
