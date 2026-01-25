// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

package indexes

import (
	"errors"
	"io"
)

var ErrNotFound = errors.New("indexes: key not found")

// Addr is a key in the index.
type Addr string

func (a Addr) String() string { return string(a) }

// Index provides key-value lookups.
type Index[V any] interface {
	// Get retrieves the value at addr.
	Get(Addr) (V, error)

	// Set stores a value at addr.
	Set(Addr, V) error

	// Delete removes the value at addr.
	Delete(Addr) error

	// Has checks if a key exists.
	Has(Addr) (bool, error)

	io.Closer
}

// SeqIndex stores int64 sequence numbers as values and tracks indexing progress.
type SeqIndex interface {
	Index[int64]

	// GetSeq returns the current indexed sequence.
	GetSeq() (int64, error)

	// SetSeq updates the current indexed sequence.
	SetSeq(int64) error
}
