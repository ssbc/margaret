// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

package indexes

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync"

	margaret "github.com/ssbc/margaret/v2"
)

// Persisted is a simple file-backed int64 index.
// File format: 8-byte seq header + repeated (2-byte keyLen + key + 8-byte value) records.
type Persisted struct {
	mu     sync.RWMutex
	file   *os.File
	data   map[Addr]int64
	seq    int64
	closed bool
}

// OpenPersisted opens or creates a file-backed sequence index.
func OpenPersisted(path string) (*Persisted, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("indexes: open: %w", err)
	}

	p := &Persisted{
		file: f,
		data: make(map[Addr]int64),
		seq:  margaret.SeqEmpty,
	}

	if err := p.load(); err != nil {
		f.Close()
		return nil, err
	}

	return p, nil
}

func (p *Persisted) load() error {
	stat, err := p.file.Stat()
	if err != nil {
		return err
	}

	if stat.Size() == 0 {
		return nil
	}

	// Read seq
	var seqBuf [8]byte
	if _, err := p.file.ReadAt(seqBuf[:], 0); err != nil {
		return fmt.Errorf("indexes: read seq: %w", err)
	}
	p.seq = int64(binary.BigEndian.Uint64(seqBuf[:]))

	// Read entries
	pos := int64(8)
	for pos < stat.Size() {
		var lenBuf [2]byte
		if _, err := p.file.ReadAt(lenBuf[:], pos); err != nil {
			return fmt.Errorf("indexes: read key len: %w", err)
		}
		keyLen := binary.BigEndian.Uint16(lenBuf[:])
		pos += 2

		key := make([]byte, keyLen)
		if _, err := p.file.ReadAt(key, pos); err != nil {
			return fmt.Errorf("indexes: read key: %w", err)
		}
		pos += int64(keyLen)

		var valBuf [8]byte
		if _, err := p.file.ReadAt(valBuf[:], pos); err != nil {
			return fmt.Errorf("indexes: read val: %w", err)
		}
		val := int64(binary.BigEndian.Uint64(valBuf[:]))
		pos += 8

		p.data[Addr(key)] = val
	}

	return nil
}

func (p *Persisted) save() error {
	if err := p.file.Truncate(0); err != nil {
		return err
	}

	var seqBuf [8]byte
	binary.BigEndian.PutUint64(seqBuf[:], uint64(p.seq))
	if _, err := p.file.WriteAt(seqBuf[:], 0); err != nil {
		return err
	}

	pos := int64(8)
	for addr, val := range p.data {
		key := []byte(addr)

		var lenBuf [2]byte
		binary.BigEndian.PutUint16(lenBuf[:], uint16(len(key)))
		if _, err := p.file.WriteAt(lenBuf[:], pos); err != nil {
			return err
		}
		pos += 2

		if _, err := p.file.WriteAt(key, pos); err != nil {
			return err
		}
		pos += int64(len(key))

		var valBuf [8]byte
		binary.BigEndian.PutUint64(valBuf[:], uint64(val))
		if _, err := p.file.WriteAt(valBuf[:], pos); err != nil {
			return err
		}
		pos += 8
	}

	return p.file.Sync()
}

func (p *Persisted) Get(addr Addr) (int64, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return 0, errors.New("indexes: closed")
	}

	v, ok := p.data[addr]
	if !ok {
		return 0, ErrNotFound
	}
	return v, nil
}

func (p *Persisted) Set(addr Addr, v int64) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return errors.New("indexes: closed")
	}

	p.data[addr] = v
	return p.save()
}

func (p *Persisted) Delete(addr Addr) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return errors.New("indexes: closed")
	}

	delete(p.data, addr)
	return p.save()
}

func (p *Persisted) Has(addr Addr) (bool, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return false, errors.New("indexes: closed")
	}

	_, ok := p.data[addr]
	return ok, nil
}

func (p *Persisted) GetSeq() (int64, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.seq, nil
}

func (p *Persisted) SetSeq(seq int64) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return errors.New("indexes: closed")
	}

	p.seq = seq
	return p.save()
}

func (p *Persisted) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}
	p.closed = true

	return p.file.Close()
}
