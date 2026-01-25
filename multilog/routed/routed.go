// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

// Package routed provides a MultiLog backed by one offset2 log per address.
package routed

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	margaret "github.com/ssbc/margaret/v2"
	"github.com/ssbc/margaret/v2/multilog"
	"github.com/ssbc/margaret/v2/offset2"
)

// Routed is a MultiLog backed by offset2 logs in subdirectories.
type Routed[T margaret.Encodeable] struct {
	mu     sync.RWMutex
	path   string
	logs   map[multilog.Addr]*offset2.Log[T]
	closed bool
}

// NewRouted creates a new file-backed multilog at the given path.
func NewRouted[T margaret.Encodeable](path string) (*Routed[T], error) {
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, fmt.Errorf("multilog: mkdir: %w", err)
	}

	return &Routed[T]{
		path: path,
		logs: make(map[multilog.Addr]*offset2.Log[T]),
	}, nil
}

func (r *Routed[T]) sublogPath(addr multilog.Addr) string {
	return filepath.Join(r.path, sanitizeAddr(addr))
}

func sanitizeAddr(addr multilog.Addr) string {
	s := string(addr)
	result := make([]byte, 0, len(s)*2)
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_' {
			result = append(result, c)
		} else {
			result = append(result, fmt.Sprintf("%%%02x", c)...)
		}
	}
	return string(result)
}

func unsanitizeAddr(s string) string {
	result := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		if s[i] == '%' && i+2 < len(s) {
			var b byte
			fmt.Sscanf(s[i:i+3], "%%%02x", &b)
			result = append(result, b)
			i += 2
		} else {
			result = append(result, s[i])
		}
	}
	return string(result)
}

// Get returns the log at addr, opening or creating it as needed.
func (r *Routed[T]) Get(addr multilog.Addr) (margaret.Log[T], error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil, errors.New("multilog: closed")
	}

	if log, ok := r.logs[addr]; ok {
		return log, nil
	}

	log, err := offset2.Open[T](r.sublogPath(addr))
	if err != nil {
		return nil, fmt.Errorf("multilog: open sublog %s: %w", addr, err)
	}

	r.logs[addr] = log
	return log, nil
}

// List returns all addresses that have logs on disk.
func (r *Routed[T]) List() ([]multilog.Addr, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.closed {
		return nil, errors.New("multilog: closed")
	}

	entries, err := os.ReadDir(r.path)
	if err != nil {
		return nil, fmt.Errorf("multilog: readdir: %w", err)
	}

	var addrs []multilog.Addr
	for _, e := range entries {
		if e.IsDir() {
			addrs = append(addrs, multilog.Addr(unsanitizeAddr(e.Name())))
		}
	}

	return addrs, nil
}

// Has checks if a log exists at addr.
func (r *Routed[T]) Has(addr multilog.Addr) (bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.closed {
		return false, errors.New("multilog: closed")
	}

	if _, ok := r.logs[addr]; ok {
		return true, nil
	}

	_, err := os.Stat(r.sublogPath(addr))
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// Delete removes the log at addr.
func (r *Routed[T]) Delete(addr multilog.Addr) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return errors.New("multilog: closed")
	}

	if log, ok := r.logs[addr]; ok {
		if err := log.Close(); err != nil {
			return fmt.Errorf("multilog: close sublog: %w", err)
		}
		delete(r.logs, addr)
	}

	path := r.sublogPath(addr)
	if err := os.RemoveAll(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("multilog: remove: %w", err)
	}

	return nil
}

// Close closes all open sublogs.
func (r *Routed[T]) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil
	}
	r.closed = true

	var errs []error
	for _, log := range r.logs {
		if err := log.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	r.logs = nil

	return errors.Join(errs...)
}

// Path returns the base directory path.
func (r *Routed[T]) Path() string {
	return r.path
}

var _ multilog.MultiLog[margaret.Encodeable] = (*Routed[margaret.Encodeable])(nil)
