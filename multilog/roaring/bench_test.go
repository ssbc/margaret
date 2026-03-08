// SPDX-FileCopyrightText: 2024 The margaret Authors
//
// SPDX-License-Identifier: MIT

package roaring_test

import (
	"context"
	"fmt"
	iofs "io/fs"
	"math/rand/v2"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	margaret "github.com/ssbc/margaret/v2"
	"github.com/ssbc/margaret/v2/internal/persist"
	pbadger "github.com/ssbc/margaret/v2/internal/persist/badger"
	"github.com/ssbc/margaret/v2/internal/persist/bbolt"
	"github.com/ssbc/margaret/v2/internal/persist/fs"
	"github.com/ssbc/margaret/v2/internal/persist/mkv"
	"github.com/ssbc/margaret/v2/multilog"
	"github.com/ssbc/margaret/v2/multilog/roaring"
)

const (
	numSublogs       = 100
	entriesPerSublog = 5000
)

type backendFactory struct {
	name string
	make func(dir string) (persist.Saver, error)
}

var backends = []backendFactory{
	{"fs", func(dir string) (persist.Saver, error) {
		return fs.New(dir), nil
	}},
	{"mkv", func(dir string) (persist.Saver, error) {
		return mkv.New(dir)
	}},
	{"bbolt", func(dir string) (persist.Saver, error) {
		return bbolt.New(dir)
	}},
	{"bbolt-nosync", func(dir string) (persist.Saver, error) {
		return bbolt.New(dir, bbolt.NoSync)
	}},
	{"badger", func(dir string) (persist.Saver, error) {
		return pbadger.New(dir)
	}},
}

// generateAddrs returns numSublogs unique addresses.
func generateAddrs(n int) []multilog.Addr {
	addrs := make([]multilog.Addr, n)
	for i := range addrs {
		addrs[i] = multilog.Addr(fmt.Sprintf("author-%04d", i))
	}
	return addrs
}

// generateEntries returns a slice of entriesPerSublog sorted, unique int64s
// spread across a realistic range (simulating sequence numbers in a large log).
func generateEntries(n int, rng *rand.Rand) []int64 {
	seen := make(map[int64]struct{}, n)
	entries := make([]int64, 0, n)
	for len(entries) < n {
		v := rng.Int64N(1_000_000)
		if _, ok := seen[v]; !ok {
			seen[v] = struct{}{}
			entries = append(entries, v)
		}
	}
	return entries
}

// BenchmarkRoaringAppend measures populating sublogs with entries.
func BenchmarkRoaringAppend(b *testing.B) {
	addrs := generateAddrs(numSublogs)

	for _, be := range backends {
		b.Run(be.name, func(b *testing.B) {
			for b.Loop() {
				dir := b.TempDir()
				store, err := be.make(filepath.Join(dir, be.name))
				if err != nil {
					b.Fatal(err)
				}
				ml := roaring.NewStore(store)
				rng := rand.New(rand.NewPCG(42, 0))

				for _, addr := range addrs {
					sub, err := ml.Get(addr)
					if err != nil {
						b.Fatal(err)
					}
					entries := generateEntries(entriesPerSublog, rng)
					for _, v := range entries {
						_, err = sub.Append(seq(v))
						if err != nil {
							b.Fatal(err)
						}
					}
				}

				if err := ml.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkRoaringFlush measures flushing all dirty bitmaps to storage.
// Populates once, then on each iteration appends one new entry per sublog
// to re-dirty all bitmaps before flushing.
func BenchmarkRoaringFlush(b *testing.B) {
	addrs := generateAddrs(numSublogs)

	for _, be := range backends {
		b.Run(be.name, func(b *testing.B) {
			dir := b.TempDir()
			store, err := be.make(filepath.Join(dir, be.name))
			if err != nil {
				b.Fatal(err)
			}
			ml := roaring.NewStore(store)
			rng := rand.New(rand.NewPCG(42, 0))

			// Initial population (untimed).
			for _, addr := range addrs {
				sub, err := ml.Get(addr)
				if err != nil {
					b.Fatal(err)
				}
				entries := generateEntries(entriesPerSublog, rng)
				for _, v := range entries {
					if _, err := sub.Append(seq(v)); err != nil {
						b.Fatal(err)
					}
				}
			}
			// First flush to establish baseline on disk.
			if err := ml.Flush(); err != nil {
				b.Fatal(err)
			}

			nextVal := int64(2_000_000) // beyond generateEntries range
			b.ResetTimer()
			for b.Loop() {
				// Re-dirty all sublogs by appending one entry each.
				for _, addr := range addrs {
					sub, _ := ml.Get(addr)
					sub.Append(seq(nextVal))
					nextVal++
				}
				if err := ml.Flush(); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			ml.Close()
		})
	}
}

// BenchmarkRoaringReopen measures closing (flush) + reopening and loading all sublogs.
func BenchmarkRoaringReopen(b *testing.B) {
	addrs := generateAddrs(numSublogs)

	for _, be := range backends {
		b.Run(be.name, func(b *testing.B) {
			// Populate once outside timing.
			dir := b.TempDir()
			store, err := be.make(filepath.Join(dir, be.name))
			if err != nil {
				b.Fatal(err)
			}
			ml := roaring.NewStore(store)
			rng := rand.New(rand.NewPCG(42, 0))

			for _, addr := range addrs {
				sub, err := ml.Get(addr)
				if err != nil {
					b.Fatal(err)
				}
				entries := generateEntries(entriesPerSublog, rng)
				for _, v := range entries {
					if _, err := sub.Append(seq(v)); err != nil {
						b.Fatal(err)
					}
				}
			}
			if err := ml.Close(); err != nil {
				b.Fatal(err)
			}

			b.ResetTimer()
			for b.Loop() {
				store, err := be.make(filepath.Join(dir, be.name))
				if err != nil {
					b.Fatal(err)
				}
				ml := roaring.NewStore(store)

				// Load all sublogs.
				for _, addr := range addrs {
					sub, err := ml.Get(addr)
					if err != nil {
						b.Fatal(err)
					}
					if sub.Seq() != int64(entriesPerSublog-1) {
						b.Fatalf("expected seq %d, got %d", entriesPerSublog-1, sub.Seq())
					}
				}

				if err := ml.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// dirSize returns the total size of all files under root in bytes.
func dirSize(root string) (int64, error) {
	var total int64
	err := filepath.WalkDir(root, func(_ string, d iofs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		total += info.Size()
		return nil
	})
	return total, err
}

// TestStorageOverhead measures on-disk footprint for each backend.
// Not a benchmark — runs once and prints human-readable sizes.
func TestStorageOverhead(t *testing.T) {
	// Skip bbolt-nosync since it produces identical on-disk data as bbolt.
	storageBackends := []backendFactory{
		{"fs", func(dir string) (persist.Saver, error) {
			return fs.New(dir), nil
		}},
		{"mkv", func(dir string) (persist.Saver, error) {
			return mkv.New(dir)
		}},
		{"bbolt", func(dir string) (persist.Saver, error) {
			return bbolt.New(dir)
		}},
		{"badger", func(dir string) (persist.Saver, error) {
			return pbadger.New(dir)
		}},
	}

	addrs := generateAddrs(numSublogs)

	for _, be := range storageBackends {
		t.Run(be.name, func(t *testing.T) {
			r := require.New(t)
			dir := t.TempDir()
			storeDir := filepath.Join(dir, be.name)
			store, err := be.make(storeDir)
			r.NoError(err)

			ml := roaring.NewStore(store)
			rng := rand.New(rand.NewPCG(42, 0))

			for _, addr := range addrs {
				sub, err := ml.Get(addr)
				r.NoError(err)
				entries := generateEntries(entriesPerSublog, rng)
				for _, v := range entries {
					_, err = sub.Append(seq(v))
					r.NoError(err)
				}
			}
			r.NoError(ml.Close())

			size, err := dirSize(storeDir)
			r.NoError(err)
			t.Logf("%-10s  %d sublogs x %d entries = %d bytes (%.1f KB per sublog)",
				be.name, numSublogs, entriesPerSublog, size, float64(size)/float64(numSublogs)/1024)
		})
	}
}

// BenchmarkRoaringQuery measures reopen + load + iterate all entries.
// Each iteration reopens the store from disk so the benchmark captures
// deserialization cost from each backend, not just in-memory bitmap iteration.
func BenchmarkRoaringQuery(b *testing.B) {
	addrs := generateAddrs(numSublogs)

	for _, be := range backends {
		b.Run(be.name, func(b *testing.B) {
			// Populate and persist once (untimed).
			dir := b.TempDir()
			store, err := be.make(filepath.Join(dir, be.name))
			if err != nil {
				b.Fatal(err)
			}
			ml := roaring.NewStore(store)
			rng := rand.New(rand.NewPCG(42, 0))

			for _, addr := range addrs {
				sub, err := ml.Get(addr)
				if err != nil {
					b.Fatal(err)
				}
				entries := generateEntries(entriesPerSublog, rng)
				for _, v := range entries {
					if _, err := sub.Append(seq(v)); err != nil {
						b.Fatal(err)
					}
				}
			}
			if err := ml.Close(); err != nil {
				b.Fatal(err)
			}

			b.ResetTimer()
			for b.Loop() {
				// Reopen from disk.
				store, err := be.make(filepath.Join(dir, be.name))
				if err != nil {
					b.Fatal(err)
				}
				ml := roaring.NewStore(store)

				// Load all sublogs + iterate every entry.
				totalEntries := 0
				for _, addr := range addrs {
					sub, err := ml.Get(addr)
					if err != nil {
						b.Fatal(err)
					}
					qry := sub.Query()
					for range qry.Iter() {
						totalEntries++
					}
					if err := qry.Err(); err != nil {
						b.Fatal(err)
					}
				}
				if totalEntries != numSublogs*entriesPerSublog {
					b.Fatalf("expected %d entries, got %d", numSublogs*entriesPerSublog, totalEntries)
				}

				if err := ml.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// --- Stress Tests ---

// stressBackends excludes bbolt-nosync (same code path as bbolt for correctness).
var stressBackends = []backendFactory{
	{"fs", func(dir string) (persist.Saver, error) {
		return fs.New(dir), nil
	}},
	{"mkv", func(dir string) (persist.Saver, error) {
		return mkv.New(dir)
	}},
	{"bbolt", func(dir string) (persist.Saver, error) {
		return bbolt.New(dir)
	}},
	{"badger", func(dir string) (persist.Saver, error) {
		return pbadger.New(dir)
	}},
}

// TestStressConcurrentAppendAndQuery exercises concurrent writers and readers
// on the same multilog. Multiple goroutines append to different sublogs while
// another set of goroutines continuously queries existing sublogs.
func TestStressConcurrentAppendAndQuery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	const (
		writers          = 10
		readers          = 5
		appendsPerWriter = 500
		duration         = 3 * time.Second
	)

	for _, be := range stressBackends {
		t.Run(be.name, func(t *testing.T) {
			r := require.New(t)
			dir := t.TempDir()
			store, err := be.make(filepath.Join(dir, be.name))
			r.NoError(err)

			ml := roaring.NewStore(store)
			ctx, cancel := context.WithTimeout(context.Background(), duration)
			defer cancel()

			var wg sync.WaitGroup

			// Writers: each goroutine owns its own sublog address.
			for w := range writers {
				wg.Add(1)
				go func() {
					defer wg.Done()
					addr := multilog.Addr(fmt.Sprintf("writer-%d", w))
					sub, err := ml.Get(addr)
					if err != nil {
						t.Errorf("writer %d: Get: %v", w, err)
						return
					}
					for i := range appendsPerWriter {
						if ctx.Err() != nil {
							return
						}
						if _, err := sub.Append(seq(int64(w*10000 + i))); err != nil {
							t.Errorf("writer %d: Append: %v", w, err)
							return
						}
					}
				}()
			}

			// Readers: repeatedly query random sublogs while writers are active.
			for rd := range readers {
				wg.Add(1)
				go func() {
					defer wg.Done()
					rng := rand.New(rand.NewPCG(uint64(rd), 0))
					for ctx.Err() == nil {
						addr := multilog.Addr(fmt.Sprintf("writer-%d", rng.IntN(writers)))
						sub, err := ml.Get(addr)
						if err != nil {
							t.Errorf("reader %d: Get: %v", rd, err)
							return
						}
						qry := sub.Query()
						for range qry.Iter() {
						}
						if err := qry.Err(); err != nil {
							t.Errorf("reader %d: Query: %v", rd, err)
							return
						}
					}
				}()
			}

			wg.Wait()
			r.NoError(ml.Close())

			// Verify: reopen and check all data survived.
			store, err = be.make(filepath.Join(dir, be.name))
			r.NoError(err)
			ml = roaring.NewStore(store)

			for w := range writers {
				addr := multilog.Addr(fmt.Sprintf("writer-%d", w))
				sub, err := ml.Get(addr)
				r.NoError(err)
				r.EqualValues(appendsPerWriter-1, sub.Seq(),
					"writer %d: expected seq %d, got %d", w, appendsPerWriter-1, sub.Seq())
			}
			r.NoError(ml.Close())
		})
	}
}

// TestStressFlushDuringAppend exercises the periodic flush goroutine
// racing with appends. Uses a short flush interval to maximize contention.
func TestStressFlushDuringAppend(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	const (
		numAuthors     = 50
		appendsPerAuth = 1000
	)

	for _, be := range stressBackends {
		t.Run(be.name, func(t *testing.T) {
			r := require.New(t)
			dir := t.TempDir()
			store, err := be.make(filepath.Join(dir, be.name))
			r.NoError(err)

			ml := roaring.NewStore(store)

			// Spawn goroutines that append rapidly.
			var wg sync.WaitGroup
			for a := range numAuthors {
				wg.Add(1)
				go func() {
					defer wg.Done()
					addr := multilog.Addr(fmt.Sprintf("author-%d", a))
					sub, err := ml.Get(addr)
					if err != nil {
						t.Errorf("author %d: Get: %v", a, err)
						return
					}
					for i := range appendsPerAuth {
						if _, err := sub.Append(seq(int64(a*100000 + i))); err != nil {
							t.Errorf("author %d: Append %d: %v", a, i, err)
							return
						}
					}
				}()
			}

			// Manually trigger flushes in a tight loop while writers are active.
			done := make(chan struct{})
			go func() {
				for {
					select {
					case <-done:
						return
					default:
						ml.Flush()
						time.Sleep(time.Millisecond)
					}
				}
			}()

			wg.Wait()
			close(done)
			r.NoError(ml.Close())

			// Verify all data.
			store, err = be.make(filepath.Join(dir, be.name))
			r.NoError(err)
			ml = roaring.NewStore(store)

			for a := range numAuthors {
				addr := multilog.Addr(fmt.Sprintf("author-%d", a))
				sub, err := ml.Get(addr)
				r.NoError(err)
				r.EqualValues(appendsPerAuth-1, sub.Seq(),
					"author %d: expected seq %d, got %d", a, appendsPerAuth-1, sub.Seq())
			}
			r.NoError(ml.Close())
		})
	}
}

// TestStressLargeSublog tests a single sublog with a very large bitmap
// (100K entries) to exercise serialization/deserialization with big payloads.
func TestStressLargeSublog(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	const largeN = 100_000

	for _, be := range stressBackends {
		t.Run(be.name, func(t *testing.T) {
			r := require.New(t)
			dir := t.TempDir()
			storeDir := filepath.Join(dir, be.name)
			store, err := be.make(storeDir)
			r.NoError(err)

			ml := roaring.NewStore(store)

			sub, err := ml.Get("big")
			r.NoError(err)

			rng := rand.New(rand.NewPCG(99, 0))
			entries := generateEntries(largeN, rng)
			for _, v := range entries {
				_, err = sub.Append(seq(v))
				r.NoError(err)
			}
			r.EqualValues(largeN-1, sub.Seq())
			r.NoError(ml.Close())

			// Measure size.
			size, err := dirSize(storeDir)
			r.NoError(err)
			t.Logf("%-10s  1 sublog x %d entries = %d bytes (%.1f KB)",
				be.name, largeN, size, float64(size)/1024)

			// Reopen and verify.
			store, err = be.make(storeDir)
			r.NoError(err)
			ml = roaring.NewStore(store)

			sub, err = ml.Get("big")
			r.NoError(err)
			r.EqualValues(largeN-1, sub.Seq())

			// Full query.
			qry := sub.Query()
			count := 0
			for range qry.Iter() {
				count++
			}
			r.NoError(qry.Err())
			r.Equal(largeN, count)

			r.NoError(ml.Close())
		})
	}
}

// TestStressReopenCycle rapidly opens, appends, flushes, closes, and reopens
// to stress the open/close lifecycle.
func TestStressReopenCycle(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	const cycles = 50

	for _, be := range stressBackends {
		t.Run(be.name, func(t *testing.T) {
			r := require.New(t)
			dir := t.TempDir()
			storeDir := filepath.Join(dir, be.name)

			totalAppends := 0
			for c := range cycles {
				store, err := be.make(storeDir)
				r.NoError(err)
				ml := roaring.NewStore(store)

				sub, err := ml.Get("cycled")
				r.NoError(err)

				// Append a few entries each cycle.
				for i := range 10 {
					_, err = sub.Append(seq(int64(c*10 + i)))
					r.NoError(err)
					totalAppends++
				}

				r.NoError(ml.Close())
			}

			// Final reopen and verify.
			store, err := be.make(storeDir)
			r.NoError(err)
			ml := roaring.NewStore(store)

			sub, err := ml.Get("cycled")
			r.NoError(err)
			r.EqualValues(totalAppends-1, sub.Seq(),
				"expected %d entries after %d cycles", totalAppends, cycles)

			qry := sub.Query()
			count := 0
			for range qry.Iter() {
				count++
			}
			r.NoError(qry.Err())
			r.Equal(totalAppends, count)

			r.NoError(ml.Close())
		})
	}
}

// TestStressLiveQueryUnderLoad starts a live query, then hammers
// appends from multiple goroutines and verifies the live iterator
// sees all values.
func TestStressLiveQueryUnderLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	const (
		totalAppends = 500
		writerCount  = 5
	)

	for _, be := range stressBackends {
		t.Run(be.name, func(t *testing.T) {
			r := require.New(t)
			dir := t.TempDir()
			store, err := be.make(filepath.Join(dir, be.name))
			r.NoError(err)

			ml := roaring.NewStore(store)
			sub, err := ml.Get("live-stress")
			r.NoError(err)

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			qry := sub.Query(margaret.Live(ctx))

			// Collect values from live iterator.
			var got []int64
			var mu sync.Mutex
			readerDone := make(chan struct{})
			go func() {
				defer close(readerDone)
				for _, val := range qry.Iter() {
					mu.Lock()
					got = append(got, int64(*val))
					if len(got) >= totalAppends {
						mu.Unlock()
						cancel()
						return
					}
					mu.Unlock()
				}
			}()

			// Give iterator time to start blocking.
			time.Sleep(20 * time.Millisecond)

			// Fan-out appenders.
			var wg sync.WaitGroup
			perWriter := totalAppends / writerCount
			for w := range writerCount {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := range perWriter {
						sub.Append(seq(int64(w*perWriter + i)))
					}
				}()
			}
			wg.Wait()

			<-readerDone

			mu.Lock()
			r.Equal(totalAppends, len(got), "live query should have received all appended values")
			mu.Unlock()

			r.NoError(ml.Close())
		})
	}
}
