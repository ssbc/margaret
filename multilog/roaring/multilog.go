// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

// Package roaring provides a MultiLog backed by roaring bitmaps.
//
// Each sublog stores a set of int64 sequence numbers from a source log,
// compressed using roaring bitmaps. This is ideal for building indexes
// like "which entries in the main log belong to this author?"
package roaring

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/dgraph-io/sroar"

	margaret "github.com/ssbc/margaret/v2"
	"github.com/ssbc/margaret/v2/internal/persist"
	"github.com/ssbc/margaret/v2/internal/seqobsv"
	"github.com/ssbc/margaret/v2/multilog"
)

// NewStore returns a new roaring bitmap backed multilog.
// It uses the provided persist.Saver to store serialized bitmaps.
// Dirty bitmaps are flushed to storage periodically (every 13s) and on close.
func NewStore(store persist.Saver) *MultiLog {
	done := make(chan struct{})
	ml := &MultiLog{
		store:   store,
		sublogs: make(map[multilog.Addr]*sublog),

		done:          done,
		batcherClosed: make(chan struct{}),
		tickPersist:   time.NewTicker(13 * time.Second),
	}
	go ml.writeBatches()
	return ml
}

func (ml *MultiLog) writeBatches() {
	for {
		select {
		case <-ml.tickPersist.C:
		case <-ml.done:
			close(ml.batcherClosed)
			return
		}
		err := ml.Flush()
		if err != nil {
			log.Println("roaring: flush trigger failed:", err)
		}
	}
}

// Flush persists all dirty sublogs to the store.
func (ml *MultiLog) Flush() error {
	ml.mu.Lock()
	defer ml.mu.Unlock()
	return ml.flushAllSublogs()
}

func (ml *MultiLog) flushAllSublogs() error {
	// collect dirty sublogs but DON'T clear the dirty flag yet
	var dirty []persist.KeyValuePair
	var dirtyAddrs []multilog.Addr
	for addr, sl := range ml.sublogs {
		if sl.dirty {
			dirty = append(dirty, persist.KeyValuePair{
				Key:   persist.Key(addr),
				Value: sl.bmap.ToBuffer(),
			})
			dirtyAddrs = append(dirtyAddrs, addr)
		}
	}
	if len(dirty) == 0 {
		return nil
	}

	// persist first, THEN clear dirty flags.
	// Previously dirty was cleared before PutMultiple, so a crash during
	// persist would lose data permanently (dirty=false means no retry).
	err := ml.store.PutMultiple(dirty)
	if err != nil {
		return err
	}

	for _, addr := range dirtyAddrs {
		if sl, ok := ml.sublogs[addr]; ok {
			sl.dirty = false
		}
	}
	return nil
}

// MultiLog is a collection of sublogs backed by roaring bitmaps.
// Each sublog stores a compressed set of int64 sequence number references.
type MultiLog struct {
	store persist.Saver

	mu      sync.Mutex
	sublogs map[multilog.Addr]*sublog

	done          chan struct{}
	batcherClosed chan struct{}
	tickPersist   *time.Ticker
}

// Get returns the sublog at addr, creating it if necessary.
func (ml *MultiLog) Get(addr multilog.Addr) (margaret.Log[*Seq], error) {
	ml.mu.Lock()
	defer ml.mu.Unlock()
	return ml.openSublog(addr)
}

// openSublog opens or creates a sublog. Caller must hold ml.mu.
func (ml *MultiLog) openSublog(addr multilog.Addr) (*sublog, error) {
	if sl, ok := ml.sublogs[addr]; ok {
		return sl, nil
	}

	pk := persist.Key(addr)

	bmap, err := ml.loadBitmap(pk)
	if errors.Is(err, persist.ErrNotFound) {
		bmap = sroar.NewBitmap()
	} else if err != nil {
		return nil, err
	}

	card := bmap.GetCardinality()
	var obsV uint64
	if card > 0 {
		obsV = uint64(card)
	}

	sl := &sublog{
		mlog: ml,
		key:  pk,
		seq:  seqobsv.New(obsV),
		bmap: bmap,
	}
	ml.sublogs[addr] = sl
	return sl, nil
}

// LoadInternalBitmap returns a copy of the raw roaring bitmap for the given address.
// It flushes pending writes first to ensure consistency.
func (ml *MultiLog) LoadInternalBitmap(addr multilog.Addr) (*sroar.Bitmap, error) {
	if err := ml.Flush(); err != nil {
		return nil, err
	}
	bmap, err := ml.loadBitmap([]byte(addr))
	if err != nil {
		if errors.Is(err, persist.ErrNotFound) {
			return nil, multilog.ErrNotFound
		}
		return nil, err
	}
	return bmap, nil
}

func (ml *MultiLog) loadBitmap(key []byte) (*sroar.Bitmap, error) {
	data, err := ml.store.Get(key)
	if err != nil {
		return nil, fmt.Errorf("roaring: load bitmap %s: %w", key, err)
	}
	return sroar.FromBuffer(data), nil
}

// Has checks if a sublog exists at addr.
func (ml *MultiLog) Has(addr multilog.Addr) (bool, error) {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	if _, ok := ml.sublogs[addr]; ok {
		return true, nil
	}

	_, err := ml.store.Get(persist.Key(addr))
	if errors.Is(err, persist.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// Delete removes the sublog at addr.
func (ml *MultiLog) Delete(addr multilog.Addr) error {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	if sl, ok := ml.sublogs[addr]; ok {
		sl.deleted = true
		sl.seq = seqobsv.New(0)
		delete(ml.sublogs, addr)
	}

	return ml.store.Delete(persist.Key(addr))
}

// List returns all addresses that have sublogs.
func (ml *MultiLog) List() ([]multilog.Addr, error) {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	if err := ml.loadAll(); err != nil {
		return nil, err
	}

	var list []multilog.Addr
	for addr, sl := range ml.sublogs {
		if sl.bmap.GetCardinality() > 0 {
			list = append(list, addr)
		}
	}
	return list, nil
}

func (ml *MultiLog) loadAll() error {
	keys, err := ml.store.List()
	if err != nil {
		return fmt.Errorf("roaring: list keys: %w", err)
	}
	for _, k := range keys {
		if _, err := ml.openSublog(multilog.Addr(k)); err != nil {
			return fmt.Errorf("roaring: open sublog %s: %w", k, err)
		}
	}
	return nil
}

// Close flushes all dirty sublogs and closes the store.
func (ml *MultiLog) Close() error {
	close(ml.done)
	ml.tickPersist.Stop()
	<-ml.batcherClosed

	if err := ml.Flush(); err != nil {
		return fmt.Errorf("roaring: close flush: %w", err)
	}

	return ml.store.Close()
}

// SublogStats holds statistics for a single sublog bitmap.
type SublogStats struct {
	Addr           multilog.Addr
	Cardinality    int     // number of set bits
	SerializedSize int     // bytes when serialized via ToBuffer
	MinValue       uint64  // smallest sequence number in bitmap
	MaxValue       uint64  // largest sequence number in bitmap
	Density        float64 // cardinality / (max - min + 1), 1.0 = fully dense
}

// Stats holds aggregate statistics for the entire multilog.
type Stats struct {
	NumSublogs          int
	TotalCardinality    int64
	TotalSerializedSize int64
	AvgCardinality      float64
	AvgSerializedSize   float64
	AvgDensity          float64
	MinCardinality      int
	MaxCardinality      int
	MinSerializedSize   int
	MaxSerializedSize   int
	Sublogs             []SublogStats
}

// Stats returns statistics about all loaded sublogs.
// Call after loading sublogs (e.g. via List or Get) for complete results.
func (ml *MultiLog) Stats() Stats {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	var s Stats
	s.MinCardinality = int(^uint(0) >> 1) // max int
	s.MinSerializedSize = int(^uint(0) >> 1)

	for addr, sl := range ml.sublogs {
		card := sl.bmap.GetCardinality()
		buf := sl.bmap.ToBuffer()
		serialSize := len(buf)

		ss := SublogStats{
			Addr:           addr,
			Cardinality:    card,
			SerializedSize: serialSize,
		}

		if card > 0 {
			ss.MinValue = sl.bmap.Minimum()
			ss.MaxValue = sl.bmap.Maximum()
			span := ss.MaxValue - ss.MinValue + 1
			ss.Density = float64(card) / float64(span)
		}

		s.Sublogs = append(s.Sublogs, ss)
		s.TotalCardinality += int64(card)
		s.TotalSerializedSize += int64(serialSize)

		if card < s.MinCardinality {
			s.MinCardinality = card
		}
		if card > s.MaxCardinality {
			s.MaxCardinality = card
		}
		if serialSize < s.MinSerializedSize {
			s.MinSerializedSize = serialSize
		}
		if serialSize > s.MaxSerializedSize {
			s.MaxSerializedSize = serialSize
		}
	}

	s.NumSublogs = len(s.Sublogs)
	if s.NumSublogs > 0 {
		s.AvgCardinality = float64(s.TotalCardinality) / float64(s.NumSublogs)
		s.AvgSerializedSize = float64(s.TotalSerializedSize) / float64(s.NumSublogs)

		var totalDensity float64
		for _, ss := range s.Sublogs {
			totalDensity += ss.Density
		}
		s.AvgDensity = totalDensity / float64(s.NumSublogs)
	} else {
		s.MinCardinality = 0
		s.MinSerializedSize = 0
	}

	return s
}

var _ multilog.MultiLog[*Seq] = (*MultiLog)(nil)
