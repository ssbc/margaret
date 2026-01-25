// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

package indexes_test

import (
	"bytes"
	"encoding/binary"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	margaret "github.com/ssbc/margaret/v2"
	"github.com/ssbc/margaret/v2/indexes"
	"github.com/ssbc/margaret/v2/offset2"
)

// testEntry is a simple test type for log entries.
type testEntry struct {
	Name  string
	Value int64
}

func (te *testEntry) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer
	nameBytes := []byte(te.Name)
	binary.Write(&buf, binary.BigEndian, int32(len(nameBytes)))
	buf.Write(nameBytes)
	binary.Write(&buf, binary.BigEndian, int64(te.Value))
	return buf.Bytes(), nil
}

func (te *testEntry) UnmarshalBinary(data []byte) error {
	r := bytes.NewReader(data)
	var nameLen int32
	if err := binary.Read(r, binary.BigEndian, &nameLen); err != nil {
		return err
	}
	nameBytes := make([]byte, nameLen)
	if _, err := r.Read(nameBytes); err != nil {
		return err
	}
	te.Name = string(nameBytes)
	var val int64
	if err := binary.Read(r, binary.BigEndian, &val); err != nil {
		return err
	}
	te.Value = val
	return nil
}

func TestMemoryIndex(t *testing.T) {
	r := require.New(t)
	a := assert.New(t)

	idx := indexes.NewMemory[int64]()
	defer idx.Close()

	// Get non-existent key
	_, err := idx.Get("missing")
	a.ErrorIs(err, indexes.ErrNotFound)

	// Set and get
	r.NoError(idx.Set("foo", 42))
	v, err := idx.Get("foo")
	r.NoError(err)
	a.Equal(int64(42), v)

	// Has
	ok, err := idx.Has("foo")
	r.NoError(err)
	a.True(ok)

	ok, err = idx.Has("bar")
	r.NoError(err)
	a.False(ok)

	// Delete
	r.NoError(idx.Delete("foo"))
	_, err = idx.Get("foo")
	a.ErrorIs(err, indexes.ErrNotFound)

	// SeqIndex methods
	seq, err := idx.GetSeq()
	r.NoError(err)
	a.Equal(margaret.SeqEmpty, seq)

	r.NoError(idx.SetSeq(5))
	seq, err = idx.GetSeq()
	r.NoError(err)
	a.Equal(int64(5), seq)
}

func TestPersistedIndex(t *testing.T) {
	r := require.New(t)
	a := assert.New(t)

	path := filepath.Join(t.TempDir(), "test.idx")

	idx, err := indexes.OpenPersisted(path)
	r.NoError(err)

	// Set some values
	r.NoError(idx.Set("alpha", 10))
	r.NoError(idx.Set("beta", 20))
	r.NoError(idx.SetSeq(1))

	v, err := idx.Get("alpha")
	r.NoError(err)
	a.Equal(int64(10), v)

	// Close and reopen to verify persistence
	r.NoError(idx.Close())

	idx2, err := indexes.OpenPersisted(path)
	r.NoError(err)
	defer idx2.Close()

	v, err = idx2.Get("alpha")
	r.NoError(err)
	a.Equal(int64(10), v)

	v, err = idx2.Get("beta")
	r.NoError(err)
	a.Equal(int64(20), v)

	seq, err := idx2.GetSeq()
	r.NoError(err)
	a.Equal(int64(1), seq)
}

func TestSinkIndex(t *testing.T) {
	r := require.New(t)
	a := assert.New(t)

	log, err := offset2.Open[*testEntry](t.TempDir())
	r.NoError(err)
	defer log.Close()

	// Append entries
	for _, e := range []*testEntry{
		{"alice", 1},
		{"bob", 2},
		{"alice", 3},
	} {
		_, err := log.Append(e)
		r.NoError(err)
	}

	// Create index: name -> last seen seq
	idx := indexes.NewMemory[int64]()
	defer idx.Close()

	sink := indexes.NewSinkIndex[*testEntry](idx, func(seq int64, val *testEntry) (indexes.Addr, int64, bool) {
		return indexes.Addr(val.Name), seq, true
	})

	r.NoError(sink.Index(log))

	// alice was last seen at seq 2
	v, err := idx.Get("alice")
	r.NoError(err)
	a.Equal(int64(2), v)

	// bob was last seen at seq 1
	v, err = idx.Get("bob")
	r.NoError(err)
	a.Equal(int64(1), v)
}

func TestSinkIndexIncremental(t *testing.T) {
	r := require.New(t)
	a := assert.New(t)

	log, err := offset2.Open[*testEntry](t.TempDir())
	r.NoError(err)
	defer log.Close()

	// Append initial entries
	for _, e := range []*testEntry{{"a", 1}, {"b", 2}} {
		_, err := log.Append(e)
		r.NoError(err)
	}

	idx := indexes.NewMemory[int64]()
	defer idx.Close()

	seqTracker := indexes.NewMemory[int64]()
	defer seqTracker.Close()

	sink := indexes.NewSinkIndex[*testEntry](idx, func(seq int64, val *testEntry) (indexes.Addr, int64, bool) {
		return indexes.Addr(val.Name), int64(val.Value), true
	}).WithSeqTracking(seqTracker)

	// First indexing pass
	r.NoError(sink.Index(log))

	v, err := idx.Get("a")
	r.NoError(err)
	a.Equal(int64(1), v)

	trackedSeq, err := seqTracker.GetSeq()
	r.NoError(err)
	a.Equal(int64(1), trackedSeq)

	// Append more entries
	for _, e := range []*testEntry{{"c", 3}, {"d", 4}} {
		_, err := log.Append(e)
		r.NoError(err)
	}

	// Incremental indexing — should only process seq 2 and 3
	r.NoError(sink.Index(log))

	v, err = idx.Get("c")
	r.NoError(err)
	a.Equal(int64(3), v)

	v, err = idx.Get("d")
	r.NoError(err)
	a.Equal(int64(4), v)

	trackedSeq, err = seqTracker.GetSeq()
	r.NoError(err)
	a.Equal(int64(3), trackedSeq)
}
