// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

package legacyflumeoffset

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeLFOEntry appends one LFO-format entry to f and returns the offset where it was written.
func writeLFOEntry(t *testing.T, f *os.File, data []byte) int64 {
	t.Helper()

	where, err := f.Seek(0, 2) // SeekEnd
	require.NoError(t, err)

	sz := uint32(len(data))

	// leading size
	require.NoError(t, binary.Write(f, binary.BigEndian, sz))
	// data
	_, err = f.Write(data)
	require.NoError(t, err)
	// trailing size
	require.NoError(t, binary.Write(f, binary.BigEndian, sz))
	// next file offset
	next := uint32(where) + 3*4 + sz
	require.NoError(t, binary.Write(f, binary.BigEndian, next))

	return where
}

func TestReadAll(t *testing.T) {
	r := require.New(t)
	a := assert.New(t)

	dir := t.TempDir()
	path := filepath.Join(dir, "log.offset")

	// Create a synthetic LFO file with 3 entries.
	f, err := os.Create(path)
	r.NoError(err)

	entries := []string{
		`{"key":"%abc.sha256","value":{"type":"post","text":"hello"}}`,
		`{"key":"%def.sha256","value":{"type":"post","text":"world"}}`,
		`{"key":"%ghi.sha256","value":{"type":"contact","contact":"@foo"}}`,
	}
	for _, e := range entries {
		writeLFOEntry(t, f, []byte(e))
	}
	r.NoError(f.Close())

	// Read them back.
	reader, err := OpenReadOnly(path)
	r.NoError(err)
	defer reader.Close()

	var got []string
	for data, err := range reader.ReadAll() {
		r.NoError(err)
		got = append(got, string(data))
	}

	a.Equal(len(entries), len(got), "entry count mismatch")
	for i, want := range entries {
		a.Equal(want, got[i], "entry %d mismatch", i)
	}
}

func TestReadAllEmpty(t *testing.T) {
	r := require.New(t)

	dir := t.TempDir()
	path := filepath.Join(dir, "empty.offset")

	f, err := os.Create(path)
	r.NoError(err)
	r.NoError(f.Close())

	reader, err := OpenReadOnly(path)
	r.NoError(err)
	defer reader.Close()

	count := 0
	for _, err := range reader.ReadAll() {
		r.NoError(err)
		count++
	}
	r.Equal(0, count)
}

func TestReadAllEarlyBreak(t *testing.T) {
	r := require.New(t)

	dir := t.TempDir()
	path := filepath.Join(dir, "log.offset")

	f, err := os.Create(path)
	r.NoError(err)

	for i := 0; i < 10; i++ {
		writeLFOEntry(t, f, []byte(`{"n":`+string(rune('0'+i))+`}`))
	}
	r.NoError(f.Close())

	reader, err := OpenReadOnly(path)
	r.NoError(err)
	defer reader.Close()

	count := 0
	for _, err := range reader.ReadAll() {
		r.NoError(err)
		count++
		if count == 3 {
			break
		}
	}
	r.Equal(3, count)
}
