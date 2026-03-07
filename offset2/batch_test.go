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

func TestAppendBatchBasic(t *testing.T) {
	r := require.New(t)

	log, err := Open[*testEvent](t.TempDir())
	r.NoError(err)
	defer log.Close()

	batch := []*testEvent{
		{Foo: "a", Bar: 1},
		{Foo: "b", Bar: 2},
		{Foo: "c", Bar: 3},
		{Foo: "d", Bar: 4},
		{Foo: "e", Bar: 5},
	}

	seqs, err := log.AppendBatch(batch)
	r.NoError(err)
	r.Len(seqs, len(batch))

	// Verify contiguous sequences starting at 0.
	for i, seq := range seqs {
		r.Equal(int64(i), seq, "seq %d", i)
	}

	// Verify Get returns correct values.
	for i, ev := range batch {
		got, err := log.Get(int64(i))
		r.NoError(err)
		r.Equal(*ev, *got, "entry %d", i)
	}

	// Verify Seq() equals last sequence.
	r.Equal(int64(len(batch)-1), log.Seq())
}

func TestAppendBatchEmpty(t *testing.T) {
	r := require.New(t)

	log, err := Open[*testEvent](t.TempDir())
	r.NoError(err)
	defer log.Close()

	seqs, err := log.AppendBatch(nil)
	r.NoError(err)
	r.Nil(seqs)
	r.Equal(margaret.SeqEmpty, log.Seq())

	seqs, err = log.AppendBatch([]*testEvent{})
	r.NoError(err)
	r.Nil(seqs)
	r.Equal(margaret.SeqEmpty, log.Seq())
}

func TestAppendBatchSingleEntry(t *testing.T) {
	r := require.New(t)

	log, err := Open[*testEvent](t.TempDir())
	r.NoError(err)
	defer log.Close()

	seqs, err := log.AppendBatch([]*testEvent{{Foo: "solo", Bar: 42}})
	r.NoError(err)
	r.Equal([]int64{0}, seqs)
	r.Equal(int64(0), log.Seq())

	got, err := log.Get(0)
	r.NoError(err)
	r.Equal("solo", got.Foo)
}

func TestAppendBatchAfterAppend(t *testing.T) {
	r := require.New(t)

	log, err := Open[*testEvent](t.TempDir())
	r.NoError(err)
	defer log.Close()

	// Individual appends first.
	for i, ev := range []testEvent{{"x", 10}, {"y", 20}} {
		seq, err := log.Append(&ev)
		r.NoError(err)
		r.Equal(int64(i), seq)
	}

	// Batch append should continue from seq 2.
	batch := []*testEvent{{Foo: "a", Bar: 1}, {Foo: "b", Bar: 2}, {Foo: "c", Bar: 3}}
	seqs, err := log.AppendBatch(batch)
	r.NoError(err)
	r.Equal([]int64{2, 3, 4}, seqs)

	r.Equal(int64(4), log.Seq())

	// Verify all entries.
	got, err := log.Get(0)
	r.NoError(err)
	r.Equal("x", got.Foo)

	got, err = log.Get(2)
	r.NoError(err)
	r.Equal("a", got.Foo)

	got, err = log.Get(4)
	r.NoError(err)
	r.Equal("c", got.Foo)
}

func TestAppendBatchClosed(t *testing.T) {
	r := require.New(t)

	log, err := Open[*testEvent](t.TempDir())
	r.NoError(err)
	r.NoError(log.Close())

	_, err = log.AppendBatch([]*testEvent{{Foo: "nope", Bar: 0}})
	r.ErrorIs(err, ErrClosed)
}

func TestAppendBatchPersistence(t *testing.T) {
	r := require.New(t)
	dir := t.TempDir()

	log, err := Open[*testEvent](dir)
	r.NoError(err)

	batch := []*testEvent{
		{Foo: "p1", Bar: 100},
		{Foo: "p2", Bar: 200},
		{Foo: "p3", Bar: 300},
	}
	_, err = log.AppendBatch(batch)
	r.NoError(err)
	r.NoError(log.Close())

	// Reopen and verify.
	log, err = Open[*testEvent](dir)
	r.NoError(err)
	defer log.Close()

	r.Equal(int64(2), log.Seq())
	for i, ev := range batch {
		got, err := log.Get(int64(i))
		r.NoError(err)
		r.Equal(*ev, *got)
	}
}

func TestAppendBatchLiveQuery(t *testing.T) {
	r := require.New(t)
	a := assert.New(t)

	log, err := Open[*testEvent](t.TempDir())
	r.NoError(err)
	defer log.Close()

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

	// Give the live query time to start.
	time.Sleep(50 * time.Millisecond)

	// Batch append.
	batch := []*testEvent{
		{Foo: "a", Bar: 1},
		{Foo: "b", Bar: 2},
		{Foo: "c", Bar: 3},
	}
	seqs, err := log.AppendBatch(batch)
	r.NoError(err)
	r.Equal([]int64{0, 1, 2}, seqs)

	// Wait for live entries to arrive.
	time.Sleep(100 * time.Millisecond)
	cancel()
	<-done

	mu.Lock()
	a.Equal([]testEvent{{"a", 1}, {"b", 2}, {"c", 3}}, got)
	mu.Unlock()
}

func TestAppendBatchMixedWithLive(t *testing.T) {
	r := require.New(t)
	a := assert.New(t)

	log, err := Open[*testEvent](t.TempDir())
	r.NoError(err)
	defer log.Close()

	// Individual append first.
	_, err = log.Append(&testEvent{"before", 0})
	r.NoError(err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start live from seq > 0.
	qry := log.Query(margaret.Live(ctx), margaret.Gt(0))

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

	// Batch append.
	batch := []*testEvent{{Foo: "x", Bar: 1}, {Foo: "y", Bar: 2}}
	_, err = log.AppendBatch(batch)
	r.NoError(err)

	// Single append after batch.
	_, err = log.Append(&testEvent{"z", 3})
	r.NoError(err)

	time.Sleep(100 * time.Millisecond)
	cancel()
	<-done

	mu.Lock()
	a.Equal([]testEvent{{"x", 1}, {"y", 2}, {"z", 3}}, got)
	mu.Unlock()
}

func TestAppendBatchQueryIterator(t *testing.T) {
	r := require.New(t)

	log, err := Open[*testEvent](t.TempDir())
	r.NoError(err)
	defer log.Close()

	batch := []*testEvent{
		{Foo: "a", Bar: 1},
		{Foo: "b", Bar: 2},
		{Foo: "c", Bar: 3},
	}
	_, err = log.AppendBatch(batch)
	r.NoError(err)

	// Snapshot query should see all entries.
	qry := log.Query()
	var count int
	for seq, val := range qry.Iter() {
		r.Equal(int64(count), seq)
		r.Equal(*batch[count], *val)
		count++
	}
	r.NoError(qry.Err())
	r.Equal(len(batch), count)
}

func BenchmarkAppendSingle(b *testing.B) {
	log, err := Open[*testEvent](b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer log.Close()

	ev := &testEvent{Foo: "bench", Bar: 42}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := log.Append(ev); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAppendBatch100(b *testing.B) {
	log, err := Open[*testEvent](b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer log.Close()

	batch := make([]*testEvent, 100)
	for i := range batch {
		batch[i] = &testEvent{Foo: "bench", Bar: i}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := log.AppendBatch(batch); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAppendSingle100(b *testing.B) {
	log, err := Open[*testEvent](b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer log.Close()

	events := make([]*testEvent, 100)
	for i := range events {
		events[i] = &testEvent{Foo: "bench", Bar: i}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, ev := range events {
			if _, err := log.Append(ev); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkAppendBatch128(b *testing.B) {
	log, err := Open[*testEvent](b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer log.Close()

	batch := make([]*testEvent, 128)
	for i := range batch {
		batch[i] = &testEvent{Foo: "bench", Bar: i}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := log.AppendBatch(batch); err != nil {
			b.Fatal(err)
		}
	}
}
