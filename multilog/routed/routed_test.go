// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

package routed_test

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ssbc/margaret/v2/multilog"
	"github.com/ssbc/margaret/v2/multilog/routed"
	"github.com/ssbc/margaret/v2/offset2"
)

type testMsg struct {
	Author string
	Seq    int64
}

func (m *testMsg) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer
	ab := []byte(m.Author)
	binary.Write(&buf, binary.BigEndian, int32(len(ab)))
	buf.Write(ab)
	binary.Write(&buf, binary.BigEndian, m.Seq)
	return buf.Bytes(), nil
}

func (m *testMsg) UnmarshalBinary(data []byte) error {
	r := bytes.NewReader(data)
	var nameLen int32
	if err := binary.Read(r, binary.BigEndian, &nameLen); err != nil {
		return err
	}
	ab := make([]byte, nameLen)
	if _, err := r.Read(ab); err != nil {
		return err
	}
	m.Author = string(ab)
	return binary.Read(r, binary.BigEndian, &m.Seq)
}

func TestRoutedBasic(t *testing.T) {
	r := require.New(t)
	a := assert.New(t)

	mlog, err := routed.NewRouted[*testMsg](t.TempDir())
	r.NoError(err)
	defer mlog.Close()

	// Initially empty
	addrs, err := mlog.List()
	r.NoError(err)
	a.Empty(addrs)

	ok, err := mlog.Has("alice")
	r.NoError(err)
	a.False(ok)

	// Get creates the sublog
	sub, err := mlog.Get("alice")
	r.NoError(err)

	_, err = sub.Append(&testMsg{"alice", 1})
	r.NoError(err)
	_, err = sub.Append(&testMsg{"alice", 2})
	r.NoError(err)

	a.EqualValues(1, sub.Seq())

	// Now listed
	addrs, err = mlog.List()
	r.NoError(err)
	a.Equal([]multilog.Addr{"alice"}, addrs)

	ok, err = mlog.Has("alice")
	r.NoError(err)
	a.True(ok)

	// Get existing entry
	val, err := sub.Get(0)
	r.NoError(err)
	a.Equal("alice", val.Author)
	a.EqualValues(1, val.Seq)

	// Delete
	r.NoError(mlog.Delete("alice"))
	ok, err = mlog.Has("alice")
	r.NoError(err)
	a.False(ok)
}

func TestRoutedMultipleSublogs(t *testing.T) {
	r := require.New(t)
	a := assert.New(t)

	mlog, err := routed.NewRouted[*testMsg](t.TempDir())
	r.NoError(err)
	defer mlog.Close()

	for _, name := range []string{"alice", "bob", "carol"} {
		sub, err := mlog.Get(multilog.Addr(name))
		r.NoError(err)

		_, err = sub.Append(&testMsg{name, 1})
		r.NoError(err)
	}

	addrs, err := mlog.List()
	r.NoError(err)
	a.Len(addrs, 3)
}

func TestRoutedSanitizeAddr(t *testing.T) {
	r := require.New(t)
	a := assert.New(t)

	dir := t.TempDir()
	mlog, err := routed.NewRouted[*testMsg](dir)
	r.NoError(err)

	// Test with special characters in address
	addr := multilog.Addr("@abc/def=.ed25519")
	sub, err := mlog.Get(addr)
	r.NoError(err)

	_, err = sub.Append(&testMsg{"test", 1})
	r.NoError(err)

	// Should be listed with original address
	addrs, err := mlog.List()
	r.NoError(err)
	a.Equal([]multilog.Addr{addr}, addrs)

	// Close and reopen to verify persistence of sanitized names
	r.NoError(mlog.Close())

	mlog2, err := routed.NewRouted[*testMsg](dir)
	r.NoError(err)
	defer mlog2.Close()

	addrs, err = mlog2.List()
	r.NoError(err)
	a.Equal([]multilog.Addr{addr}, addrs)
}

func TestRoutedWithSink(t *testing.T) {
	r := require.New(t)
	a := assert.New(t)

	mlog, err := routed.NewRouted[*testMsg](t.TempDir())
	r.NoError(err)
	defer mlog.Close()

	sink := multilog.NewSink[*testMsg](mlog, func(seq int64, val *testMsg, ml multilog.MultiLog[*testMsg]) error {
		sub, err := ml.Get(multilog.Addr(val.Author))
		if err != nil {
			return err
		}
		_, err = sub.Append(val)
		return err
	})

	// Create a source log and append entries
	srcDir := t.TempDir()
	srcLog, err := offset2.Open[*testMsg](srcDir)
	r.NoError(err)
	defer srcLog.Close()

	for _, msg := range []*testMsg{
		{"alice", 1},
		{"bob", 1},
		{"alice", 2},
		{"bob", 2},
		{"carol", 1},
	} {
		_, err := srcLog.Append(msg)
		r.NoError(err)
	}

	// Index into multilog
	r.NoError(sink.Index(srcLog))

	// Verify sublogs
	addrs, err := mlog.List()
	r.NoError(err)
	a.Len(addrs, 3)

	aliceSub, err := mlog.Get("alice")
	r.NoError(err)
	a.EqualValues(1, aliceSub.Seq()) // 2 entries: seq 0, 1

	bobSub, err := mlog.Get("bob")
	r.NoError(err)
	a.EqualValues(1, bobSub.Seq()) // 2 entries

	carolSub, err := mlog.Get("carol")
	r.NoError(err)
	a.EqualValues(0, carolSub.Seq()) // 1 entry
}
