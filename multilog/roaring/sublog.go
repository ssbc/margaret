// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

package roaring

import (
	"context"
	"fmt"
	"strings"

	"github.com/dgraph-io/sroar"

	margaret "github.com/ssbc/margaret/v2"
	"github.com/ssbc/margaret/v2/internal/persist"
	"github.com/ssbc/margaret/v2/internal/seqobsv"
)

type sublog struct {
	mlog *MultiLog

	key     persist.Key
	seq     *seqobsv.Observable
	bmap    *sroar.Bitmap
	dirty   bool
	deleted bool
}

// Seq returns the latest sequence number (cardinality - 1), or SeqEmpty if empty.
func (sl *sublog) Seq() int64 {
	v := sl.seq.Seq()
	if v == 0 {
		return margaret.SeqEmpty
	}
	return v - 1
}

// Get returns the seq-th sequence number stored in the bitmap.
func (sl *sublog) Get(seq int64) (*Seq, error) {
	sl.mlog.mu.Lock()
	defer sl.mlog.mu.Unlock()

	if sl.deleted {
		return nil, fmt.Errorf("roaring: sublog deleted")
	}
	if seq < 0 {
		return nil, margaret.ErrOutOfBounds
	}

	v, err := sl.bmap.Select(uint64(seq))
	if err != nil {
		return nil, margaret.ErrOutOfBounds
	}
	r := Seq(v)
	return &r, nil
}

// Append adds a sequence number to the bitmap.
// The value must be a *Seq with a non-negative int64 value.
func (sl *sublog) Append(v *Seq) (int64, error) {
	sl.mlog.mu.Lock()
	defer sl.mlog.mu.Unlock()

	if sl.deleted {
		return margaret.SeqEmpty, fmt.Errorf("roaring: sublog deleted")
	}

	val := int64(*v)
	if val < 0 {
		return margaret.SeqEmpty, fmt.Errorf("roaring: can only store non-negative numbers")
	}

	sl.bmap.Set(uint64(val))
	sl.dirty = true
	sl.seq.Inc()

	newSeq := int64(sl.bmap.GetCardinality()) - 1
	return newSeq, nil
}

// Query returns an iterator over the sequence numbers in this sublog.
func (sl *sublog) Query(opts ...margaret.QueryOption) margaret.QueryIterator[*Seq] {
	cfg, err := margaret.ApplyQueryOptions(opts...)
	if err != nil {
		return margaret.NewFailedIterator[*Seq](err)
	}

	sl.mlog.mu.Lock()
	if sl.deleted {
		sl.mlog.mu.Unlock()
		return margaret.NewFailedIterator[*Seq](fmt.Errorf("roaring: sublog deleted"))
	}

	card := int64(sl.bmap.GetCardinality())
	sl.mlog.mu.Unlock()

	if cfg.Live {
		return sl.liveQuery(cfg, card)
	}
	return sl.staticQuery(cfg, card)
}

func (sl *sublog) staticQuery(cfg margaret.QueryConfig, card int64) margaret.QueryIterator[*Seq] {
	start, end := cfg.Bounds(card - 1)
	if card == 0 {
		return margaret.NewIterWrapper[*Seq](func(yield func(int64, *Seq) bool) {})
	}

	var iterErr error
	iter := func(yield func(int64, *Seq) bool) {
		count := 0
		if cfg.Reverse {
			for i := end; i >= start; i-- {
				if cfg.Limit > 0 && count >= cfg.Limit {
					return
				}
				sl.mlog.mu.Lock()
				v, err := sl.bmap.Select(uint64(i))
				sl.mlog.mu.Unlock()
				if err != nil {
					if strings.Contains(err.Error(), "is not less than the cardinality:") {
						return
					}
					iterErr = fmt.Errorf("roaring: select %d: %w", i, err)
					return
				}
				r := Seq(v)
				count++
				if !yield(i, &r) {
					return
				}
			}
		} else {
			for i := start; i <= end; i++ {
				if cfg.Limit > 0 && count >= cfg.Limit {
					return
				}
				sl.mlog.mu.Lock()
				v, err := sl.bmap.Select(uint64(i))
				sl.mlog.mu.Unlock()
				if err != nil {
					if strings.Contains(err.Error(), "is not less than the cardinality:") {
						return
					}
					iterErr = fmt.Errorf("roaring: select %d: %w", i, err)
					return
				}
				r := Seq(v)
				count++
				if !yield(i, &r) {
					return
				}
			}
		}
	}
	return margaret.NewLiveIterWrapper[*Seq](iter, func() error { return iterErr })
}

func (sl *sublog) liveQuery(cfg margaret.QueryConfig, card int64) margaret.QueryIterator[*Seq] {
	ctx := cfg.Ctx
	if ctx == nil {
		ctx = context.Background()
	}

	start := int64(0)
	if cfg.Gt != nil {
		start = *cfg.Gt + 1
	}
	if cfg.Gte != nil && *cfg.Gte > start {
		start = *cfg.Gte
	}

	var liveErr error
	iter := func(yield func(int64, *Seq) bool) {
		nextSeq := start
		count := 0
		for {
			if cfg.Limit > 0 && count >= cfg.Limit {
				return
			}

			sl.mlog.mu.Lock()
			v, err := sl.bmap.Select(uint64(nextSeq))
			sl.mlog.mu.Unlock()

			if err != nil {
				if !strings.Contains(err.Error(), "is not less than the cardinality:") {
					liveErr = fmt.Errorf("roaring: select %d: %w", nextSeq, err)
					return
				}
				// Wait for new data
				select {
				case <-sl.seq.WaitFor(uint64(nextSeq)):
					continue
				case <-ctx.Done():
					liveErr = ctx.Err()
					return
				}
			}

			r := Seq(v)
			count++
			if !yield(nextSeq, &r) {
				return
			}
			nextSeq++
		}
	}

	return margaret.NewLiveIterWrapper[*Seq](iter, func() error {
		return liveErr
	})
}

// Close is a no-op for sublogs; the parent MultiLog manages persistence.
func (sl *sublog) Close() error {
	return nil
}

var _ margaret.Log[*Seq] = (*sublog)(nil)
