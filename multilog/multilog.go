// SPDX-License-Identifier: MIT

package multilog

import (
	"io"

	"github.com/pkg/errors"
	"go.cryptoscope.co/margaret"
	"go.cryptoscope.co/margaret/indexes"
)

var ErrSublogDeleted = errors.Errorf("multilog: stored sublog was deleted. please re-open")

// MultiLog is a collection of logs, keyed by a indexes.Addr
// TODO maybe only call this log to avoid multilog.MultiLog?
type MultiLog interface {
	Get(indexes.Addr) (margaret.Log, error)
	List() ([]indexes.Addr, error)

	io.Closer

	Flush() error

	// Delete removes all entries related to that log
	Delete(indexes.Addr) error
}

func Has(mlog MultiLog, addr indexes.Addr) (bool, error) {
	slog, err := mlog.Get(addr)
	if err != nil {
		return false, err
	}

	seqVal, err := slog.Seq().Value()
	if err != nil {
		return false, err
	}

	return seqVal.(margaret.BaseSeq) != margaret.SeqEmpty, nil
}
