// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

// Package multilog provides a collection of logs keyed by address.
package multilog

import (
	"errors"
	"fmt"
	"io"
	"iter"

	margaret "github.com/ssbc/margaret/v2"
)

var ErrNotFound = errors.New("multilog: sublog not found")

// Addr is a key in the multilog.
type Addr string

func (a Addr) String() string { return string(a) }

// MultiLog is a collection of logs keyed by Addr.
type MultiLog[T margaret.Encodeable] interface {
	// Get returns the log at addr, creating it if necessary.
	Get(Addr) (margaret.Log[T], error)

	// List returns all addresses with logs.
	List() ([]Addr, error)

	// Has checks if a log exists at addr.
	Has(Addr) (bool, error)

	// Delete removes the log at addr.
	Delete(Addr) error

	io.Closer
}

// Func is a processing function for routing log entries to sublogs.
type Func[T margaret.Encodeable] func(seq int64, value T, mlog MultiLog[T]) error

// Sink processes a source log and routes entries to sublogs.
type Sink[T margaret.Encodeable] struct {
	mlog    MultiLog[T]
	process Func[T]
}

// NewSink creates an indexing sink.
func NewSink[T margaret.Encodeable](mlog MultiLog[T], f Func[T]) *Sink[T] {
	return &Sink[T]{mlog: mlog, process: f}
}

// Index processes all entries from the source log.
func (s *Sink[T]) Index(source margaret.Log[T]) error {
	qry := source.Query()
	for seq, val := range qry.Iter() {
		if err := s.process(seq, val, s.mlog); err != nil {
			return fmt.Errorf("multilog: index seq %d: %w", seq, err)
		}
	}
	return qry.Err()
}

// IndexFrom processes entries starting from the given sequence.
func (s *Sink[T]) IndexFrom(source margaret.Log[T], from int64) error {
	qry := source.Query(margaret.Gte(from))
	for seq, val := range qry.Iter() {
		if err := s.process(seq, val, s.mlog); err != nil {
			return fmt.Errorf("multilog: index seq %d: %w", seq, err)
		}
	}
	return qry.Err()
}

// All returns a nested iterator over all entries across all sublogs.
func All[T margaret.Encodeable](mlog MultiLog[T]) iter.Seq2[Addr, iter.Seq2[int64, T]] {
	return func(yield func(Addr, iter.Seq2[int64, T]) bool) {
		addrs, err := mlog.List()
		if err != nil {
			return
		}

		for _, addr := range addrs {
			log, err := mlog.Get(addr)
			if err != nil {
				continue
			}

			if !yield(addr, log.Query().Iter()) {
				return
			}
		}
	}
}
