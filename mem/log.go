// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

// Package mem provides an in-memory append-only log.
package mem

import (
	"context"
	"fmt"
	"io"
	"sync"

	margaret "github.com/ssbc/margaret/v2"
)

type memlogElem[T margaret.Encodeable] struct {
	v    T
	seq  int64
	next *memlogElem[T]
	prev *memlogElem[T]
	wait chan struct{} // closed when next is set
}

// waitNext blocks until a new element is appended or the context is cancelled.
// The mutex is temporarily released to allow appends.
func (el *memlogElem[T]) waitNext(ctx context.Context, mu *sync.Mutex) (*memlogElem[T], error) {
	err := func() error {
		mu.Unlock()
		defer mu.Lock()

		select {
		case <-el.wait:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}()
	if err != nil {
		return el, err
	}
	return el.next, nil
}

type memlog[T margaret.Encodeable] struct {
	mu         sync.Mutex
	head, tail *memlogElem[T]
	closed     bool
}

// New returns a new in-memory log.
func New[T margaret.Encodeable]() margaret.Log[T] {
	root := &memlogElem[T]{
		seq:  margaret.SeqEmpty,
		wait: make(chan struct{}),
	}

	return &memlog[T]{
		head: root,
		tail: root,
	}
}

func (log *memlog[T]) Close() error {
	log.mu.Lock()
	defer log.mu.Unlock()
	if log.closed {
		return io.ErrClosedPipe
	}
	log.closed = true
	return nil
}

func (log *memlog[T]) Seq() int64 {
	log.mu.Lock()
	defer log.mu.Unlock()
	return log.tail.seq
}

func (log *memlog[T]) Get(s int64) (T, error) {
	var empty T
	log.mu.Lock()
	defer log.mu.Unlock()
	if log.closed {
		return empty, io.ErrClosedPipe
	}

	cur := log.head
	for cur.seq < s && cur.next != nil {
		cur = cur.next
	}

	if cur.seq != s {
		return empty, margaret.ErrOutOfBounds
	}

	return cur.v, nil
}

func (log *memlog[T]) Append(v T) (int64, error) {
	log.mu.Lock()
	defer log.mu.Unlock()
	if log.closed {
		return margaret.SeqErrored, io.ErrClosedPipe
	}

	nxt := &memlogElem[T]{
		v:    v,
		seq:  log.tail.seq + 1,
		prev: log.tail,
		wait: make(chan struct{}),
	}

	log.tail.next = nxt
	oldtail := log.tail
	log.tail = nxt

	close(oldtail.wait)

	return log.tail.seq, nil
}

func (log *memlog[T]) AppendBatch(values []T) ([]int64, error) {
	if len(values) == 0 {
		return nil, nil
	}

	log.mu.Lock()
	defer log.mu.Unlock()
	if log.closed {
		return nil, io.ErrClosedPipe
	}

	seqs := make([]int64, len(values))
	for i, v := range values {
		nxt := &memlogElem[T]{
			v:    v,
			seq:  log.tail.seq + 1,
			prev: log.tail,
			wait: make(chan struct{}),
		}

		log.tail.next = nxt
		oldtail := log.tail
		log.tail = nxt

		close(oldtail.wait)

		seqs[i] = log.tail.seq
	}
	return seqs, nil
}

func (log *memlog[T]) Query(opts ...margaret.QueryOption) margaret.QueryIterator[T] {
	cfg, err := margaret.ApplyQueryOptions(opts...)
	if err != nil {
		return margaret.NewFailedIterator[T](err)
	}

	if cfg.Reverse && cfg.Live {
		return margaret.NewFailedIterator[T](fmt.Errorf("memlog: can't do reverse and live"))
	}

	if cfg.Live {
		return log.queryLive(cfg)
	}
	return log.querySnapshot(cfg)
}

func (log *memlog[T]) querySnapshot(cfg margaret.QueryConfig) margaret.QueryIterator[T] {
	iter := func(yield func(int64, T) bool) {
		log.mu.Lock()
		defer log.mu.Unlock()

		if log.tail.seq == margaret.SeqEmpty {
			return
		}

		start, end := cfg.Bounds(log.tail.seq)
		if start > end {
			return
		}

		count := 0

		if cfg.Reverse {
			// Find the end node
			cur := log.tail
			for cur.seq > end && cur.prev != nil {
				cur = cur.prev
			}

			for cur.seq >= start {
				if cfg.Limit > 0 && count >= cfg.Limit {
					return
				}
				if cur.seq >= 0 {
					if !yield(cur.seq, cur.v) {
						return
					}
					count++
				}
				if cur.prev == nil {
					break
				}
				cur = cur.prev
			}
		} else {
			// Find the start node
			cur := log.head
			for cur.seq < start && cur.next != nil {
				cur = cur.next
			}

			for cur.seq <= end {
				if cfg.Limit > 0 && count >= cfg.Limit {
					return
				}
				if cur.seq >= 0 {
					if !yield(cur.seq, cur.v) {
						return
					}
					count++
				}
				if cur.next == nil {
					break
				}
				cur = cur.next
			}
		}
	}

	return margaret.NewIterWrapper(iter)
}

func (log *memlog[T]) queryLive(cfg margaret.QueryConfig) margaret.QueryIterator[T] {
	var iterErr error
	ctx := cfg.Ctx

	iter := func(yield func(int64, T) bool) {
		log.mu.Lock()
		defer log.mu.Unlock()

		// Determine starting position
		start, _ := cfg.Bounds(log.tail.seq)

		// Find the starting node (the node BEFORE the first one we want)
		cur := log.head
		for cur.next != nil && cur.next.seq < start {
			cur = cur.next
		}

		count := 0

		// Phase 1: drain existing entries
		for cur.next != nil {
			cur = cur.next

			if cfg.Limit > 0 && count >= cfg.Limit {
				return
			}

			if cur.seq >= start {
				if !yield(cur.seq, cur.v) {
					return
				}
				count++
			}
		}

		// Phase 2: follow new entries via wait channels
		for {
			if cfg.Limit > 0 && count >= cfg.Limit {
				return
			}

			next, err := cur.waitNext(ctx, &log.mu)
			if err != nil {
				iterErr = err
				return
			}
			cur = next

			if !yield(cur.seq, cur.v) {
				return
			}
			count++
		}
	}

	return margaret.NewLiveIterWrapper(iter, func() error { return iterErr })
}
