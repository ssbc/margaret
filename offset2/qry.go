// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

package offset2

import (
	margaret "github.com/ssbc/margaret/v2"
)

// Query returns an iterator over entries matching the query options.
func (l *Log[T]) Query(opts ...margaret.QueryOption) margaret.QueryIterator[T] {
	cfg, err := margaret.ApplyQueryOptions(opts...)
	if err != nil {
		return margaret.NewFailedIterator[T](err)
	}

	if cfg.Live {
		return l.queryLive(cfg)
	}

	return l.querySnapshot(cfg)
}

// querySnapshot iterates over a fixed range of existing entries.
func (l *Log[T]) querySnapshot(cfg margaret.QueryConfig) margaret.QueryIterator[T] {
	iter := func(yield func(int64, T) bool) {
		l.mu.RLock()
		currentSeq := l.seq
		l.mu.RUnlock()

		if currentSeq == margaret.SeqEmpty {
			return
		}

		start, end := cfg.Bounds(currentSeq)
		if start > end {
			return
		}

		count := 0

		if cfg.Reverse {
			for seq := end; seq >= start; seq-- {
				if cfg.Limit > 0 && count >= cfg.Limit {
					return
				}

				val, err := l.Get(seq)
				if err == ErrNulled {
					continue
				}
				if err != nil {
					return
				}

				if !yield(seq, val) {
					return
				}
				count++
			}
		} else {
			for seq := start; seq <= end; seq++ {
				if cfg.Limit > 0 && count >= cfg.Limit {
					return
				}

				val, err := l.Get(seq)
				if err == ErrNulled {
					continue
				}
				if err != nil {
					return
				}

				if !yield(seq, val) {
					return
				}
				count++
			}
		}
	}

	return margaret.NewIterWrapper(iter)
}

// queryLive iterates existing entries then blocks waiting for new ones.
// Stops when the context is cancelled or limit is reached.
func (l *Log[T]) queryLive(cfg margaret.QueryConfig) margaret.QueryIterator[T] {
	var iterErr error

	iter := func(yield func(int64, T) bool) {
		ctx := cfg.Ctx

		// Notification channel — buffered so appends don't block.
		// Multiple rapid appends collapse into one wake-up.
		notify := make(chan struct{}, 1)
		unregister := l.OnAppend(func(_ int64, _ T) {
			select {
			case notify <- struct{}{}:
			default:
			}
		})
		defer unregister()

		// Determine starting position
		l.mu.RLock()
		currentSeq := l.seq
		l.mu.RUnlock()

		var nextSeq int64
		if currentSeq == margaret.SeqEmpty {
			nextSeq = 0
		} else {
			start, _ := cfg.Bounds(currentSeq)
			nextSeq = start
		}

		count := 0

		// Phase 1: drain existing entries
		for nextSeq <= currentSeq {
			if cfg.Limit > 0 && count >= cfg.Limit {
				return
			}

			val, err := l.Get(nextSeq)
			if err == ErrNulled {
				nextSeq++
				continue
			}
			if err != nil {
				iterErr = err
				return
			}

			if !yield(nextSeq, val) {
				return
			}
			count++
			nextSeq++
		}

		// Phase 2: follow new entries
		for {
			if cfg.Limit > 0 && count >= cfg.Limit {
				return
			}

			select {
			case <-ctx.Done():
				iterErr = ctx.Err()
				return

			case <-notify:
				l.mu.RLock()
				currentSeq = l.seq
				l.mu.RUnlock()

				for nextSeq <= currentSeq {
					if cfg.Limit > 0 && count >= cfg.Limit {
						return
					}

					val, err := l.Get(nextSeq)
					if err == ErrNulled {
						nextSeq++
						continue
					}
					if err != nil {
						iterErr = err
						return
					}

					if !yield(nextSeq, val) {
						return
					}
					count++
					nextSeq++
				}
			}
		}
	}

	return margaret.NewLiveIterWrapper(iter, func() error { return iterErr })
}
