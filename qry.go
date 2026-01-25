// SPDX-FileCopyrightText: 2021-2026 The margaret Authors
//
// SPDX-License-Identifier: MIT

package margaret

import (
	"context"
	"fmt"
)

// QueryOption configures a query.
type QueryOption func(*QueryConfig) error

// QueryConfig holds query parameters.
type QueryConfig struct {
	Gt      *int64 // Greater than
	Gte     *int64 // Greater than or equal
	Lt      *int64 // Less than
	Lte     *int64 // Less than or equal
	Limit   int    // Max entries (0 = unlimited)
	Reverse bool   // Iterate in reverse order
	Live    bool   // Follow new entries as they are appended
	Ctx     context.Context
}

// Gt returns only entries with seq > n.
func Gt(n int64) QueryOption {
	return func(q *QueryConfig) error {
		q.Gt = &n
		return nil
	}
}

// Gte returns only entries with seq >= n.
func Gte(n int64) QueryOption {
	return func(q *QueryConfig) error {
		q.Gte = &n
		return nil
	}
}

// Lt returns only entries with seq < n.
func Lt(n int64) QueryOption {
	return func(q *QueryConfig) error {
		q.Lt = &n
		return nil
	}
}

// Lte returns only entries with seq <= n.
func Lte(n int64) QueryOption {
	return func(q *QueryConfig) error {
		q.Lte = &n
		return nil
	}
}

// Limit returns at most n entries.
func Limit(n int) QueryOption {
	return func(q *QueryConfig) error {
		q.Limit = n
		return nil
	}
}

// Reverse iterates from newest to oldest.
func Reverse(yes bool) QueryOption {
	return func(q *QueryConfig) error {
		q.Reverse = yes
		return nil
	}
}

// Live follows new entries as they are appended.
// The iterator blocks waiting for new entries until ctx is cancelled.
// Lt and Lte are ignored in live mode (there is no upper bound).
func Live(ctx context.Context) QueryOption {
	return func(q *QueryConfig) error {
		q.Live = true
		q.Ctx = ctx
		return nil
	}
}

// ApplyQueryOptions builds a QueryConfig from options.
func ApplyQueryOptions(opts ...QueryOption) (QueryConfig, error) {
	var cfg QueryConfig
	for _, opt := range opts {
		if err := opt(&cfg); err != nil {
			return cfg, err
		}
	}
	if cfg.Live && cfg.Reverse {
		return cfg, fmt.Errorf("margaret: can't do reverse and live")
	}
	return cfg, nil
}

// Bounds calculates the effective start/end sequence from config and log length.
func (cfg QueryConfig) Bounds(logSeq int64) (start, end int64) {
	start = 0
	end = logSeq

	if cfg.Gt != nil && *cfg.Gt+1 > start {
		start = *cfg.Gt + 1
	}
	if cfg.Gte != nil && *cfg.Gte > start {
		start = *cfg.Gte
	}
	if !cfg.Live {
		if cfg.Lt != nil && *cfg.Lt-1 < end {
			end = *cfg.Lt - 1
		}
		if cfg.Lte != nil && *cfg.Lte < end {
			end = *cfg.Lte
		}
	}

	return start, end
}
