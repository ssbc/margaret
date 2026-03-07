// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

package offset2

import (
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

type testEvent struct {
	Foo string
	Bar int
}

func (te testEvent) MarshalBinary() ([]byte, error) {
	return json.Marshal(te)
}

func (te *testEvent) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, te)
}

func TestReadWrite(t *testing.T) {
	r := require.New(t)

	log, err := Open[*testEvent](t.TempDir())
	r.NoError(err, "error during log creation")

	tevs := []testEvent{
		{"hello", 23},
		{"world", 42},
		{"world", 161},
		{"world", 1312},
	}
	for i, ev := range tevs {
		seq, err := log.Append(&ev)
		r.NoError(err, "failed to append event %d", i)
		r.Equal(int64(i), seq, "sequence mismatch")
	}

	for i := 0; i < len(tevs); i++ {
		ev, err := log.Get(int64(i))
		r.NoError(err, "failed to get event %d", i)
		r.Equal(tevs[i], *ev)
	}
}

// make sure that the sequence is picked up after opening an existing log
func TestWriteAndWriteAgain(t *testing.T) {
	r := require.New(t)
	where := t.TempDir()

	log, err := Open[*testEvent](where)
	r.NoError(err, "error during log creation")

	tevs := []testEvent{
		{"hello", 23},
		{"world", 42},
		{"world", 161},
		{"world", 1312},
	}
	for i, ev := range tevs {
		seq, err := log.Append(&ev)
		r.NoError(err, "failed to append event %d", i)
		r.Equal(int64(i), seq, "sequence mismatch")
	}

	// reopen
	r.NoError(log.Close())
	log, err = Open[*testEvent](where)
	r.NoError(err, "error during log reopen")

	// fill again
	for i, ev := range tevs {
		seq, err := log.Append(&ev)
		r.NoError(err, "failed to do 2nd append %d", i)
		r.Equal(int64(len(tevs)+i), seq, "sequence mismatch %d", i)
	}

	// close
	r.NoError(log.Close())

	// cant write to closed log
	_, err = log.Append(&testEvent{"closed", 666})
	r.NotNil(err)

	// reopen and verify
	log, err = Open[*testEvent](where)
	r.NoError(err, "error during log creation")

	currSeq := log.Seq()
	r.EqualValues(int64(2*len(tevs)-1), currSeq)

	// read by seq
	for i := 0; i < 2*len(tevs); i++ {
		v, err := log.Get(int64(i))
		r.NoError(err, "failed to get event %d", i)
		r.Equal(tevs[i%len(tevs)], *v)
	}

	// drain via iterator
	qry := log.Query()
	var seq int64
	for s, v := range qry.Iter() {
		t.Log(s, v)
		seq++
	}
	r.NoError(qry.Err())
	r.EqualValues(2*len(tevs), seq)

	r.NoError(log.Close())
}

// should be able to recover from journal in the future
func TestRecover(t *testing.T) {
	r := require.New(t)
	where := t.TempDir()

	log, err := Open[*testEvent](where)
	r.NoError(err, "error during log creation")

	tevs := []testEvent{
		{"hello", 23},
		{"world", 42},
		{"world", 161},
		{"world", 1312},
	}
	for i, ev := range tevs {
		seq, err := log.Append(&ev)
		r.NoError(err, "failed to append event %d", i)
		r.Equal(int64(i), seq, "sequence mismatch")
	}

	// close
	r.NoError(log.Close())

	// reopen and corrupt journal by bumping sequence
	log, err = Open[*testEvent](where)
	r.NoError(err, "error during log open")

	// simulate journal being ahead (as if a write was in progress)
	log.mu.Lock()
	log.seq++
	r.NoError(log.writeJournal())
	log.mu.Unlock()

	r.NoError(log.Close())

	// reopen should recover from journal
	log, err = Open[*testEvent](where)
	r.NoError(err, "error while recover")
	r.NotNil(log)

	v := log.Seq()
	r.EqualValues(len(tevs), v, "journal should reflect bumped seq")

	r.NoError(log.Close())
}

func TestReadCorruptLength(t *testing.T) {
	r := require.New(t)
	dir := t.TempDir()

	log, err := Open[*testEvent](dir)
	r.NoError(err)

	// Write one valid entry.
	_, err = log.Append(&testEvent{"good", 1})
	r.NoError(err)
	r.NoError(log.Close())

	// Corrupt the length prefix of the first entry to an absurd value.
	dataPath := filepath.Join(dir, "data")
	f, err := os.OpenFile(dataPath, os.O_RDWR, 0644)
	r.NoError(err)
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], 0xFFFFFFFFFFFFFFFF)
	_, err = f.WriteAt(buf[:], 0)
	r.NoError(err)
	r.NoError(f.Close())

	// Reopen — the log should open fine (corruption is in data, not journal).
	log, err = Open[*testEvent](dir)
	r.NoError(err)
	defer log.Close()

	// Get should return an error, not panic.
	_, err = log.Get(0)
	r.Error(err)
	r.Contains(err.Error(), "corrupt entry")
}
