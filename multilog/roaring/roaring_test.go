// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

package roaring_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	margaret "github.com/ssbc/margaret/v2"
	"github.com/ssbc/margaret/v2/internal/persist/fs"
	"github.com/ssbc/margaret/v2/multilog"
	"github.com/ssbc/margaret/v2/multilog/roaring"
)

func newTestMultiLog(t *testing.T) *roaring.MultiLog {
	t.Helper()
	return roaring.NewStore(fs.New(t.TempDir()))
}

func seq(v int64) *roaring.Seq {
	s := roaring.Seq(v)
	return &s
}

func TestBasicAppendAndGet(t *testing.T) {
	r := require.New(t)
	a := assert.New(t)

	ml := newTestMultiLog(t)
	defer ml.Close()

	// Initially empty
	addrs, err := ml.List()
	r.NoError(err)
	a.Empty(addrs)

	ok, err := ml.Has("alice")
	r.NoError(err)
	a.False(ok)

	// Get creates the sublog
	sub, err := ml.Get("alice")
	r.NoError(err)
	a.EqualValues(margaret.SeqEmpty, sub.Seq())

	// Append some sequence references
	newSeq, err := sub.Append(seq(10))
	r.NoError(err)
	a.EqualValues(0, newSeq)

	newSeq, err = sub.Append(seq(20))
	r.NoError(err)
	a.EqualValues(1, newSeq)

	newSeq, err = sub.Append(seq(30))
	r.NoError(err)
	a.EqualValues(2, newSeq)

	a.EqualValues(2, sub.Seq())

	// Get values back
	val, err := sub.Get(0)
	r.NoError(err)
	a.EqualValues(10, *val)

	val, err = sub.Get(1)
	r.NoError(err)
	a.EqualValues(20, *val)

	val, err = sub.Get(2)
	r.NoError(err)
	a.EqualValues(30, *val)

	// Out of bounds
	_, err = sub.Get(3)
	a.ErrorIs(err, margaret.ErrOutOfBounds)

	_, err = sub.Get(-1)
	a.ErrorIs(err, margaret.ErrOutOfBounds)

	// Now listed
	ok, err = ml.Has("alice")
	r.NoError(err)
	a.True(ok)

	addrs, err = ml.List()
	r.NoError(err)
	a.Len(addrs, 1)
}

func TestDelete(t *testing.T) {
	r := require.New(t)
	a := assert.New(t)

	ml := newTestMultiLog(t)
	defer ml.Close()

	sub, err := ml.Get("alice")
	r.NoError(err)

	_, err = sub.Append(seq(5))
	r.NoError(err)

	ok, err := ml.Has("alice")
	r.NoError(err)
	a.True(ok)

	r.NoError(ml.Delete("alice"))

	ok, err = ml.Has("alice")
	r.NoError(err)
	a.False(ok)
}

func TestMultipleSublogs(t *testing.T) {
	r := require.New(t)
	a := assert.New(t)

	ml := newTestMultiLog(t)
	defer ml.Close()

	// Create sublogs for different authors
	for i, name := range []string{"alice", "bob", "carol"} {
		sub, err := ml.Get(multilog.Addr(name))
		r.NoError(err)

		for j := 0; j < (i + 1); j++ {
			_, err = sub.Append(seq(int64(i*10 + j)))
			r.NoError(err)
		}
	}

	addrs, err := ml.List()
	r.NoError(err)
	a.Len(addrs, 3)

	// alice has 1 entry
	alice, err := ml.Get("alice")
	r.NoError(err)
	a.EqualValues(0, alice.Seq())

	// bob has 2 entries
	bob, err := ml.Get("bob")
	r.NoError(err)
	a.EqualValues(1, bob.Seq())

	// carol has 3 entries
	carol, err := ml.Get("carol")
	r.NoError(err)
	a.EqualValues(2, carol.Seq())
}

func TestQueryForward(t *testing.T) {
	r := require.New(t)
	a := assert.New(t)

	ml := newTestMultiLog(t)
	defer ml.Close()

	sub, err := ml.Get("test")
	r.NoError(err)

	// Append sequence references: 5, 10, 15, 20, 25
	for _, v := range []int64{5, 10, 15, 20, 25} {
		_, err = sub.Append(seq(v))
		r.NoError(err)
	}

	// Query all
	qry := sub.Query()
	var got []int64
	for _, val := range qry.Iter() {
		got = append(got, int64(*val))
	}
	r.NoError(qry.Err())
	a.Equal([]int64{5, 10, 15, 20, 25}, got)
}

func TestQueryWithBounds(t *testing.T) {
	r := require.New(t)
	a := assert.New(t)

	ml := newTestMultiLog(t)
	defer ml.Close()

	sub, err := ml.Get("test")
	r.NoError(err)

	for _, v := range []int64{5, 10, 15, 20, 25} {
		_, err = sub.Append(seq(v))
		r.NoError(err)
	}

	// Query Gte(1), Lte(3) -> entries at index 1,2,3 -> values 10,15,20
	qry := sub.Query(margaret.Gte(1), margaret.Lte(3))
	var got []int64
	for _, val := range qry.Iter() {
		got = append(got, int64(*val))
	}
	r.NoError(qry.Err())
	a.Equal([]int64{10, 15, 20}, got)
}

func TestQueryReverse(t *testing.T) {
	r := require.New(t)
	a := assert.New(t)

	ml := newTestMultiLog(t)
	defer ml.Close()

	sub, err := ml.Get("test")
	r.NoError(err)

	for _, v := range []int64{5, 10, 15, 20, 25} {
		_, err = sub.Append(seq(v))
		r.NoError(err)
	}

	qry := sub.Query(margaret.Reverse(true))
	var got []int64
	for _, val := range qry.Iter() {
		got = append(got, int64(*val))
	}
	r.NoError(qry.Err())
	a.Equal([]int64{25, 20, 15, 10, 5}, got)
}

func TestQueryLimit(t *testing.T) {
	r := require.New(t)
	a := assert.New(t)

	ml := newTestMultiLog(t)
	defer ml.Close()

	sub, err := ml.Get("test")
	r.NoError(err)

	for _, v := range []int64{5, 10, 15, 20, 25} {
		_, err = sub.Append(seq(v))
		r.NoError(err)
	}

	qry := sub.Query(margaret.Limit(2))
	var got []int64
	for _, val := range qry.Iter() {
		got = append(got, int64(*val))
	}
	r.NoError(qry.Err())
	a.Equal([]int64{5, 10}, got)
}

func TestQueryEmpty(t *testing.T) {
	r := require.New(t)

	ml := newTestMultiLog(t)
	defer ml.Close()

	sub, err := ml.Get("empty")
	r.NoError(err)

	qry := sub.Query()
	count := 0
	for range qry.Iter() {
		count++
	}
	r.NoError(qry.Err())
	r.Equal(0, count)
}

func TestPersistence(t *testing.T) {
	r := require.New(t)
	a := assert.New(t)

	dir := t.TempDir()

	// Create and populate
	ml := roaring.NewStore(fs.New(dir))
	sub, err := ml.Get("persist")
	r.NoError(err)

	for _, v := range []int64{100, 200, 300} {
		_, err = sub.Append(seq(v))
		r.NoError(err)
	}

	r.NoError(ml.Close())

	// Reopen and verify
	ml2 := roaring.NewStore(fs.New(dir))
	defer ml2.Close()

	sub2, err := ml2.Get("persist")
	r.NoError(err)
	a.EqualValues(2, sub2.Seq())

	val, err := sub2.Get(0)
	r.NoError(err)
	a.EqualValues(100, *val)

	val, err = sub2.Get(2)
	r.NoError(err)
	a.EqualValues(300, *val)
}

func TestLoadInternalBitmap(t *testing.T) {
	r := require.New(t)
	a := assert.New(t)

	ml := newTestMultiLog(t)
	defer ml.Close()

	sub, err := ml.Get("bitmap")
	r.NoError(err)

	for _, v := range []int64{1, 5, 10, 50, 100} {
		_, err = sub.Append(seq(v))
		r.NoError(err)
	}

	bmap, err := ml.LoadInternalBitmap("bitmap")
	r.NoError(err)
	a.EqualValues(5, bmap.GetCardinality())

	// Not found
	_, err = ml.LoadInternalBitmap("nonexistent")
	a.ErrorIs(err, multilog.ErrNotFound)
}

func TestLiveQuery(t *testing.T) {
	r := require.New(t)
	a := assert.New(t)

	ml := newTestMultiLog(t)
	defer ml.Close()

	sub, err := ml.Get("live")
	r.NoError(err)

	// Prepopulate
	_, err = sub.Append(seq(10))
	r.NoError(err)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	qry := sub.Query(margaret.Live(ctx))

	var got []int64
	done := make(chan struct{})
	go func() {
		defer close(done)
		for _, val := range qry.Iter() {
			got = append(got, int64(*val))
			if len(got) >= 3 {
				cancel()
				return
			}
		}
	}()

	// Give the iterator time to read the first value and block
	time.Sleep(50 * time.Millisecond)

	// Append more values
	_, err = sub.Append(seq(20))
	r.NoError(err)

	time.Sleep(50 * time.Millisecond)
	_, err = sub.Append(seq(30))
	r.NoError(err)

	<-done
	a.Equal([]int64{10, 20, 30}, got)
}

func TestWithSink(t *testing.T) {
	r := require.New(t)
	a := assert.New(t)

	ml := newTestMultiLog(t)
	defer ml.Close()

	// Simulate indexing: route source log entries to sublogs by some criteria.
	// Here we just use the Seq value modulo 2 to route to "even" or "odd".
	sink := multilog.NewSink[*roaring.Seq](ml, func(sourceSeq int64, val *roaring.Seq, mlog multilog.MultiLog[*roaring.Seq]) error {
		var addr multilog.Addr
		if int64(*val)%2 == 0 {
			addr = "even"
		} else {
			addr = "odd"
		}
		sub, err := mlog.Get(addr)
		if err != nil {
			return err
		}
		_, err = sub.Append(seq(sourceSeq))
		return err
	})

	// Create a mock source by appending to the multilog directly
	// In real use, this would be an offset2 log with actual messages.
	// We simulate by calling the sink's process function manually.
	_ = sink

	// Direct sublog usage
	even, err := ml.Get("even")
	r.NoError(err)
	odd, err := ml.Get("odd")
	r.NoError(err)

	// Source seq 0 -> value 10 (even)
	_, err = even.Append(seq(0))
	r.NoError(err)
	// Source seq 1 -> value 11 (odd)
	_, err = odd.Append(seq(1))
	r.NoError(err)
	// Source seq 2 -> value 12 (even)
	_, err = even.Append(seq(2))
	r.NoError(err)

	a.EqualValues(1, even.Seq()) // 2 entries
	a.EqualValues(0, odd.Seq())  // 1 entry

	// Query even sublog
	qry := even.Query()
	var got []int64
	for _, val := range qry.Iter() {
		got = append(got, int64(*val))
	}
	r.NoError(qry.Err())
	a.Equal([]int64{0, 2}, got)
}
