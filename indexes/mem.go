// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

package indexes

import (
	"errors"
	"sync"

	margaret "github.com/ssbc/margaret/v2"
)

// Memory is a goroutine-safe in-memory index.
type Memory[V any] struct {
	mu     sync.RWMutex
	data   map[Addr]V
	seq    int64
	closed bool
}

// NewMemory creates a new in-memory index.
func NewMemory[V any]() *Memory[V] {
	return &Memory[V]{
		data: make(map[Addr]V),
		seq:  margaret.SeqEmpty,
	}
}

func (m *Memory[V]) Get(addr Addr) (V, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var zero V
	if m.closed {
		return zero, errors.New("indexes: closed")
	}

	v, ok := m.data[addr]
	if !ok {
		return zero, ErrNotFound
	}
	return v, nil
}

func (m *Memory[V]) Set(addr Addr, v V) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return errors.New("indexes: closed")
	}

	m.data[addr] = v
	return nil
}

func (m *Memory[V]) Delete(addr Addr) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return errors.New("indexes: closed")
	}

	delete(m.data, addr)
	return nil
}

func (m *Memory[V]) Has(addr Addr) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return false, errors.New("indexes: closed")
	}

	_, ok := m.data[addr]
	return ok, nil
}

func (m *Memory[V]) GetSeq() (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.seq, nil
}

func (m *Memory[V]) SetSeq(seq int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.seq = seq
	return nil
}

func (m *Memory[V]) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	m.data = nil
	return nil
}

var (
	_ Index[int64] = (*Memory[int64])(nil)
	_ SeqIndex     = (*Memory[int64])(nil)
	_ SeqIndex     = (*Persisted)(nil)
)
