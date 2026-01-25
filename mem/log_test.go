// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

package mem_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	margaret "github.com/ssbc/margaret/v2"
	"github.com/ssbc/margaret/v2/mem"
	mtest "github.com/ssbc/margaret/v2/test"
)

func e(v int64) *mtest.Entry {
	entry := mtest.Entry(v)
	return &entry
}

func TestLogTest(t *testing.T) {
	t.Run("LogTest", mtest.LogTest(func(dir string) (mtest.Log, error) {
		return mem.New[*mtest.Entry](), nil
	}))
}

func TestMemBasic(t *testing.T) {
	r := require.New(t)
	a := assert.New(t)

	log := mem.New[*mtest.Entry]()
	defer log.Close()

	a.EqualValues(margaret.SeqEmpty, log.Seq())

	seq, err := log.Append(e(42))
	r.NoError(err)
	a.EqualValues(0, seq)

	seq, err = log.Append(e(99))
	r.NoError(err)
	a.EqualValues(1, seq)

	a.EqualValues(1, log.Seq())

	val, err := log.Get(0)
	r.NoError(err)
	a.EqualValues(42, *val)

	val, err = log.Get(1)
	r.NoError(err)
	a.EqualValues(99, *val)

	_, err = log.Get(2)
	a.ErrorIs(err, margaret.ErrOutOfBounds)
}

func TestMemQuery(t *testing.T) {
	r := require.New(t)
	a := assert.New(t)

	log := mem.New[*mtest.Entry]()
	defer log.Close()

	for i := 0; i < 5; i++ {
		_, err := log.Append(e(int64(i)))
		r.NoError(err)
	}

	// Simple query
	var got []int64
	for _, v := range log.Query().Iter() {
		got = append(got, int64(*v))
	}
	a.Equal([]int64{0, 1, 2, 3, 4}, got)

	// Gte
	got = nil
	for _, v := range log.Query(margaret.Gte(2)).Iter() {
		got = append(got, int64(*v))
	}
	a.Equal([]int64{2, 3, 4}, got)

	// Limit
	got = nil
	for _, v := range log.Query(margaret.Limit(3)).Iter() {
		got = append(got, int64(*v))
	}
	a.Equal([]int64{0, 1, 2}, got)

	// Gt and Lt
	got = nil
	for _, v := range log.Query(margaret.Gt(0), margaret.Lt(4)).Iter() {
		got = append(got, int64(*v))
	}
	a.Equal([]int64{1, 2, 3}, got)

	// Reverse
	got = nil
	for _, v := range log.Query(margaret.Reverse(true)).Iter() {
		got = append(got, int64(*v))
	}
	a.Equal([]int64{4, 3, 2, 1, 0}, got)

	// Reverse with limit
	got = nil
	for _, v := range log.Query(margaret.Reverse(true), margaret.Limit(2)).Iter() {
		got = append(got, int64(*v))
	}
	a.Equal([]int64{4, 3}, got)
}

func TestMemLive(t *testing.T) {
	r := require.New(t)
	a := assert.New(t)

	log := mem.New[*mtest.Entry]()
	defer log.Close()

	_, err := log.Append(e(1))
	r.NoError(err)
	_, err = log.Append(e(2))
	r.NoError(err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	qry := log.Query(margaret.Live(ctx))

	var got []int64
	var mu sync.Mutex
	done := make(chan struct{})

	go func() {
		for _, v := range qry.Iter() {
			mu.Lock()
			got = append(got, int64(*v))
			mu.Unlock()
		}
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)

	_, err = log.Append(e(3))
	r.NoError(err)
	_, err = log.Append(e(4))
	r.NoError(err)

	time.Sleep(50 * time.Millisecond)
	cancel()
	<-done

	mu.Lock()
	a.Equal([]int64{1, 2, 3, 4}, got)
	mu.Unlock()

	a.ErrorIs(qry.Err(), context.Canceled)
}

func TestMemLiveWithLimit(t *testing.T) {
	r := require.New(t)
	a := assert.New(t)

	log := mem.New[*mtest.Entry]()
	defer log.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	qry := log.Query(margaret.Live(ctx), margaret.Limit(3))

	var got []int64
	done := make(chan struct{})

	go func() {
		for _, v := range qry.Iter() {
			got = append(got, int64(*v))
		}
		close(done)
	}()

	for i := 0; i < 5; i++ {
		_, err := log.Append(e(int64(i + 1)))
		r.NoError(err)
		time.Sleep(10 * time.Millisecond)
	}

	<-done
	a.Equal(3, len(got))
	a.Equal([]int64{1, 2, 3}, got)
	a.NoError(qry.Err())
}

func TestMemLiveOnEmpty(t *testing.T) {
	r := require.New(t)
	a := assert.New(t)

	log := mem.New[*mtest.Entry]()
	defer log.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	qry := log.Query(margaret.Live(ctx))

	var got []int64
	done := make(chan struct{})

	go func() {
		for _, v := range qry.Iter() {
			got = append(got, int64(*v))
		}
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)

	_, err := log.Append(e(10))
	r.NoError(err)
	_, err = log.Append(e(20))
	r.NoError(err)

	time.Sleep(50 * time.Millisecond)
	cancel()
	<-done

	a.Equal([]int64{10, 20}, got)
}

func TestMemLiveReverseError(t *testing.T) {
	log := mem.New[*mtest.Entry]()
	defer log.Close()

	qry := log.Query(margaret.Live(context.Background()), margaret.Reverse(true))
	require.Error(t, qry.Err())
}

func TestMemEmptyQuery(t *testing.T) {
	a := assert.New(t)

	log := mem.New[*mtest.Entry]()
	defer log.Close()

	var got []int64
	for _, v := range log.Query().Iter() {
		got = append(got, int64(*v))
	}
	a.Empty(got)
}
