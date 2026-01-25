// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

package offset2

import (
	"context"
	"sync"
	"testing"
	"time"

	margaret "github.com/ssbc/margaret/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLiveBasic(t *testing.T) {
	r := require.New(t)
	a := assert.New(t)

	log, err := Open[*testEvent](t.TempDir())
	r.NoError(err)
	defer log.Close()

	// Append some initial entries
	for i, ev := range []testEvent{{"a", 1}, {"b", 2}} {
		seq, err := log.Append(&ev)
		r.NoError(err)
		r.Equal(int64(i), seq)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	qry := log.Query(margaret.Live(ctx))

	var got []testEvent
	var mu sync.Mutex
	done := make(chan struct{})

	go func() {
		for _, val := range qry.Iter() {
			mu.Lock()
			got = append(got, *val)
			mu.Unlock()
		}
		close(done)
	}()

	// Wait for existing entries to drain
	time.Sleep(50 * time.Millisecond)

	// Append more entries while live
	for _, ev := range []testEvent{{"c", 3}, {"d", 4}} {
		_, err := log.Append(&ev)
		r.NoError(err)
	}

	// Wait for live entries to arrive
	time.Sleep(50 * time.Millisecond)

	// Cancel to stop the iterator
	cancel()
	<-done

	mu.Lock()
	a.Equal([]testEvent{{"a", 1}, {"b", 2}, {"c", 3}, {"d", 4}}, got)
	mu.Unlock()

	a.ErrorIs(qry.Err(), context.Canceled)
}

func TestLiveWithGt(t *testing.T) {
	r := require.New(t)
	a := assert.New(t)

	log, err := Open[*testEvent](t.TempDir())
	r.NoError(err)
	defer log.Close()

	// Append initial entries
	for _, ev := range []testEvent{{"a", 1}, {"b", 2}, {"c", 3}} {
		_, err := log.Append(&ev)
		r.NoError(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start live query from seq > 1 (should get seq 2 onward)
	qry := log.Query(margaret.Live(ctx), margaret.Gt(1))

	var got []testEvent
	var mu sync.Mutex
	done := make(chan struct{})

	go func() {
		for _, val := range qry.Iter() {
			mu.Lock()
			got = append(got, *val)
			mu.Unlock()
		}
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)

	// Append more
	_, err = log.Append(&testEvent{"d", 4})
	r.NoError(err)

	time.Sleep(50 * time.Millisecond)
	cancel()
	<-done

	mu.Lock()
	a.Equal([]testEvent{{"c", 3}, {"d", 4}}, got)
	mu.Unlock()
}

func TestLiveWithLimit(t *testing.T) {
	r := require.New(t)
	a := assert.New(t)

	log, err := Open[*testEvent](t.TempDir())
	r.NoError(err)
	defer log.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Start live query with limit 3 on empty log
	qry := log.Query(margaret.Live(ctx), margaret.Limit(3))

	var got []testEvent
	done := make(chan struct{})

	go func() {
		for _, val := range qry.Iter() {
			got = append(got, *val)
		}
		close(done)
	}()

	// Append entries one by one
	for _, ev := range []testEvent{{"a", 1}, {"b", 2}, {"c", 3}, {"d", 4}} {
		_, err := log.Append(&ev)
		r.NoError(err)
		time.Sleep(10 * time.Millisecond)
	}

	// Iterator should stop after 3
	<-done
	a.Equal(3, len(got))
	a.Equal([]testEvent{{"a", 1}, {"b", 2}, {"c", 3}}, got)
	a.NoError(qry.Err(), "limit-based stop should not be an error")
}

func TestLiveOnEmptyLog(t *testing.T) {
	r := require.New(t)
	a := assert.New(t)

	log, err := Open[*testEvent](t.TempDir())
	r.NoError(err)
	defer log.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	qry := log.Query(margaret.Live(ctx))

	var got []testEvent
	done := make(chan struct{})

	go func() {
		for _, val := range qry.Iter() {
			got = append(got, *val)
		}
		close(done)
	}()

	// Append after query started
	time.Sleep(50 * time.Millisecond)
	_, err = log.Append(&testEvent{"first", 1})
	r.NoError(err)
	_, err = log.Append(&testEvent{"second", 2})
	r.NoError(err)

	time.Sleep(50 * time.Millisecond)
	cancel()
	<-done

	a.Equal([]testEvent{{"first", 1}, {"second", 2}}, got)
}

func TestLiveReverseError(t *testing.T) {
	r := require.New(t)

	log, err := Open[*testEvent](t.TempDir())
	r.NoError(err)
	defer log.Close()

	ctx := context.Background()
	qry := log.Query(margaret.Live(ctx), margaret.Reverse(true))
	r.Error(qry.Err(), "live+reverse should fail")
	r.Contains(qry.Err().Error(), "reverse and live")
}

func TestLiveConcurrentAppend(t *testing.T) {
	r := require.New(t)
	a := assert.New(t)

	log, err := Open[*testEvent](t.TempDir())
	r.NoError(err)
	defer log.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	n := 100
	qry := log.Query(margaret.Live(ctx), margaret.Limit(n))

	var got []testEvent
	done := make(chan struct{})

	go func() {
		for _, val := range qry.Iter() {
			got = append(got, *val)
		}
		close(done)
	}()

	// Concurrent rapid appends
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			_, err := log.Append(&testEvent{"x", i})
			a.NoError(err)
		}
	}()
	wg.Wait()

	// Wait for limit to be reached
	<-done
	a.Equal(n, len(got))
	a.NoError(qry.Err())
}
