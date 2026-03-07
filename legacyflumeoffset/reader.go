// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

// Package legacyflumeoffset reads the legacy flume offset log format
// as implemented by https://github.com/flumedb/flumelog-offset.
//
// Each entry in the file is encoded as:
//
//	[4 bytes: uint32 BE data length]
//	[N bytes: data (typically JSON)]
//	[4 bytes: uint32 BE data length (repeat)]
//	[4 bytes: uint32 BE next file offset]
//
// This package provides read-only sequential access to these entries.
package legacyflumeoffset

import (
	"encoding/binary"
	"fmt"
	"io"
	"iter"
	"os"
)

// Reader reads entries from a legacy flume offset log file.
type Reader struct {
	f *os.File
}

// OpenReadOnly opens a legacy flume offset log file for sequential reading.
func OpenReadOnly(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("legacyflumeoffset: open %s: %w", path, err)
	}
	return &Reader{f: f}, nil
}

// ReadAll returns an iterator over all raw entries in the log.
// Each yielded []byte is the raw data of one log entry (typically JSON).
// Iteration stops on EOF or on the first read error.
func (r *Reader) ReadAll() iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		var offset int64

		for {
			// Read 4-byte big-endian data length.
			var sz uint32
			sizeRd := io.NewSectionReader(r.f, offset, 4)
			err := binary.Read(sizeRd, binary.BigEndian, &sz)
			if err != nil {
				if err == io.EOF {
					return // clean end of file
				}
				yield(nil, fmt.Errorf("legacyflumeoffset: read size at offset %d: %w", offset, err))
				return
			}

			// Read the data.
			data := make([]byte, sz)
			_, err = r.f.ReadAt(data, offset+4)
			if err != nil {
				yield(nil, fmt.Errorf("legacyflumeoffset: read data at offset %d: %w", offset, err))
				return
			}

			if !yield(data, nil) {
				return
			}

			// Advance past: size(4) + data(sz) + size(4) + next_offset(4) = sz + 12
			offset += int64(sz) + 12
		}
	}
}

// Close closes the underlying file.
func (r *Reader) Close() error {
	return r.f.Close()
}
