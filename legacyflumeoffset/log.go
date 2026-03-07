// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

package legacyflumeoffset

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"sync"

	margaret "github.com/ssbc/margaret/v2"
)

// Log is a generic legacy flume offset log that implements margaret.Log[T].
// It maintains an in-memory seq→byte-offset index built by scanning the file on open.
type Log[T margaret.Encodeable] struct {
	mu      sync.RWMutex
	f       *os.File
	offsets []int64 // seq → byte offset into the file
}

// Open opens or creates a legacy flume offset log at the given file path.
// On open, the file is scanned to build the sequence-to-offset index.
func Open[T margaret.Encodeable](path string) (*Log[T], error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("legacyflumeoffset: open %s: %w", path, err)
	}

	l := &Log[T]{f: f}
	if err := l.buildIndex(); err != nil {
		f.Close()
		return nil, fmt.Errorf("legacyflumeoffset: build index: %w", err)
	}
	return l, nil
}

// buildIndex scans the file to populate the offsets slice.
func (l *Log[T]) buildIndex() error {
	var offset int64
	for {
		var sz uint32
		sizeRd := io.NewSectionReader(l.f, offset, 4)
		if err := binary.Read(sizeRd, binary.BigEndian, &sz); err != nil {
			if err == io.EOF {
				return nil // clean end of file
			}
			return fmt.Errorf("read size at offset %d: %w", offset, err)
		}
		l.offsets = append(l.offsets, offset)
		// size(4) + data(sz) + size(4) + next_offset(4) = sz + 12
		offset += int64(sz) + 12
	}
}

// Seq returns the current sequence number (index of last entry).
// Returns margaret.SeqEmpty if the log is empty.
func (l *Log[T]) Seq() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if len(l.offsets) == 0 {
		return margaret.SeqEmpty
	}
	return int64(len(l.offsets)) - 1
}

// Get retrieves the entry at the given sequence number.
func (l *Log[T]) Get(seq int64) (T, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var zero T
	if seq < 0 || int(seq) >= len(l.offsets) {
		return zero, margaret.ErrOutOfBounds
	}

	offset := l.offsets[seq]

	var sz uint32
	sizeRd := io.NewSectionReader(l.f, offset, 4)
	if err := binary.Read(sizeRd, binary.BigEndian, &sz); err != nil {
		return zero, fmt.Errorf("legacyflumeoffset: read size at seq %d: %w", seq, err)
	}

	data := make([]byte, sz)
	if _, err := l.f.ReadAt(data, offset+4); err != nil {
		return zero, fmt.Errorf("legacyflumeoffset: read data at seq %d: %w", seq, err)
	}

	val := margaret.NewValue[T]()
	if err := val.UnmarshalBinary(data); err != nil {
		return zero, fmt.Errorf("legacyflumeoffset: unmarshal at seq %d: %w", seq, err)
	}
	return val, nil
}

// Append adds a value to the log and returns its sequence number.
func (l *Log[T]) Append(v T) (int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	where, err := l.f.Seek(0, io.SeekEnd)
	if err != nil {
		return margaret.SeqErrored, err
	}

	b, err := v.MarshalBinary()
	if err != nil {
		return margaret.SeqErrored, err
	}

	sz := uint32(len(b))

	// leading size
	if err := binary.Write(l.f, binary.BigEndian, sz); err != nil {
		return margaret.SeqErrored, err
	}

	// data
	if _, err := l.f.Write(b); err != nil {
		return margaret.SeqErrored, err
	}

	// trailing size
	if err := binary.Write(l.f, binary.BigEndian, sz); err != nil {
		return margaret.SeqErrored, err
	}

	// next file offset
	if where > math.MaxUint32 {
		return margaret.SeqErrored, fmt.Errorf("legacyflumeoffset: file exceeds uint32 size limit")
	}
	next := uint32(where) + 3*4 + sz
	if err := binary.Write(l.f, binary.BigEndian, next); err != nil {
		return margaret.SeqErrored, err
	}

	seq := int64(len(l.offsets))
	l.offsets = append(l.offsets, where)
	return seq, nil
}

// Query returns an iterator over log entries matching the query options.
// Live mode is not supported and returns an error iterator.
func (l *Log[T]) Query(opts ...margaret.QueryOption) margaret.QueryIterator[T] {
	cfg, err := margaret.ApplyQueryOptions(opts...)
	if err != nil {
		return margaret.NewFailedIterator[T](err)
	}

	if cfg.Live {
		return margaret.NewFailedIterator[T](fmt.Errorf("legacyflumeoffset: live mode not supported"))
	}

	iter := func(yield func(int64, T) bool) {
		l.mu.RLock()
		currentSeq := l.Seq()
		l.mu.RUnlock()

		if currentSeq == margaret.SeqEmpty {
			return
		}

		start, end := cfg.Bounds(currentSeq)
		if start > end {
			return
		}

		count := 0

		if cfg.Reverse {
			for seq := end; seq >= start; seq-- {
				if cfg.Limit > 0 && count >= cfg.Limit {
					return
				}
				val, err := l.Get(seq)
				if err != nil {
					return
				}
				if !yield(seq, val) {
					return
				}
				count++
			}
		} else {
			for seq := start; seq <= end; seq++ {
				if cfg.Limit > 0 && count >= cfg.Limit {
					return
				}
				val, err := l.Get(seq)
				if err != nil {
					return
				}
				if !yield(seq, val) {
					return
				}
				count++
			}
		}
	}

	return margaret.NewIterWrapper(iter)
}

// Close closes the underlying file.
func (l *Log[T]) Close() error {
	return l.f.Close()
}
