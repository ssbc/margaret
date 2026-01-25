// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

package indexes

import (
	"fmt"

	margaret "github.com/ssbc/margaret/v2"
)

// SinkIndex indexes a log into a key-value store.
type SinkIndex[T margaret.Encodeable, V any] struct {
	idx     Index[V]
	extract func(seq int64, value T) (Addr, V, bool)
	seqIdx  SeqIndex // optional, for tracking progress
}

// NewSinkIndex creates an indexer that extracts (addr, value) pairs from log entries.
// The extract function returns (addr, value, ok) where ok=false skips the entry.
func NewSinkIndex[T margaret.Encodeable, V any](idx Index[V], extract func(seq int64, value T) (Addr, V, bool)) *SinkIndex[T, V] {
	return &SinkIndex[T, V]{
		idx:     idx,
		extract: extract,
	}
}

// WithSeqTracking adds sequence tracking so indexing can resume incrementally.
func (s *SinkIndex[T, V]) WithSeqTracking(seqIdx SeqIndex) *SinkIndex[T, V] {
	s.seqIdx = seqIdx
	return s
}

// Index processes all entries from the log, resuming from the last tracked sequence if available.
func (s *SinkIndex[T, V]) Index(log margaret.Log[T]) error {
	var startSeq int64 = 0

	if s.seqIdx != nil {
		seq, err := s.seqIdx.GetSeq()
		if err != nil {
			return fmt.Errorf("indexes: get seq: %w", err)
		}
		if seq != margaret.SeqEmpty {
			startSeq = seq + 1
		}
	}

	qry := log.Query(margaret.Gte(startSeq))
	for seq, val := range qry.Iter() {
		addr, v, ok := s.extract(seq, val)
		if ok {
			if err := s.idx.Set(addr, v); err != nil {
				return fmt.Errorf("indexes: set %s: %w", addr, err)
			}
		}

		if s.seqIdx != nil {
			if err := s.seqIdx.SetSeq(seq); err != nil {
				return fmt.Errorf("indexes: set seq: %w", err)
			}
		}
	}

	return qry.Err()
}
