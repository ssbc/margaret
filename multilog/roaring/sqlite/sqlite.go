package sqlite

import (
	"go.cryptoscope.co/margaret/internal/persist/sqlite"
	"go.cryptoscope.co/margaret/multilog/roaring"
)

func NewMultiLog(base string) (*roaring.MultiLog, error) {
	s, err := sqlite.New(base)
	if err != nil {
		return nil, err
	}
	return roaring.NewStore(s), nil
}
