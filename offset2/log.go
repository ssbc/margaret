// Package offset2 implements a persisted append-only log using three files.
//
// File format:
//   - data: length-prefixed entries (uint64 size + payload)
//   - ofst: uint64 offsets into data file for each entry
//   - jrnl: journal for crash recovery (current sequence)
//
// All integers are big-endian.
package offset2

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	margaret "github.com/ssbc/margaret/v2"
)

// TODO:move to margaret
var (
	ErrSeqOutOfRange = errors.New("offset2: sequence out of range")
	ErrNulled        = errors.New("offset2: entry was nulled")
	ErrClosed        = errors.New("offset2: log closed")
)

// Log is a persistent append-only log stored in offset2 format.
type Log[T margaret.Encodeable] struct {
	mu   sync.RWMutex
	path string

	data *os.File
	ofst *os.File
	jrnl *os.File

	seq int64

	// hooks for live subscription support
	hooksMu sync.RWMutex
	hooks   map[uint64]margaret.AppendHook[T]
	hookID  uint64

	closed bool
}

// Open opens or creates an offset2 log at the given directory path.
func Open[T margaret.Encodeable](path string) (*Log[T], error) {
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, fmt.Errorf("offset2: mkdir: %w", err)
	}

	data, err := os.OpenFile(filepath.Join(path, "data"), os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("offset2: open data: %w", err)
	}

	ofst, err := os.OpenFile(filepath.Join(path, "ofst"), os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		data.Close()
		return nil, fmt.Errorf("offset2: open ofst: %w", err)
	}

	jrnl, err := os.OpenFile(filepath.Join(path, "jrnl"), os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		data.Close()
		ofst.Close()
		return nil, fmt.Errorf("offset2: open jrnl: %w", err)
	}

	l := &Log[T]{
		path:  path,
		data:  data,
		ofst:  ofst,
		jrnl:  jrnl,
		seq:   margaret.SeqEmpty,
		hooks: make(map[uint64]margaret.AppendHook[T]),
	}

	if err := l.recoverJournal(); err != nil {
		l.Close()
		return nil, fmt.Errorf("offset2: recover failed: %w", err)
	}

	return l, nil
}

func (l *Log[T]) recoverJournal() error {
	// Read sequence from journal
	var seqBuf [8]byte
	n, err := l.jrnl.ReadAt(seqBuf[:], 0)
	if err == io.EOF || n == 0 {
		// Empty journal, check offset file
		stat, err := l.ofst.Stat()
		if err != nil {
			return err
		}
		numEntries := stat.Size() / 8
		if numEntries == 0 {
			l.seq = margaret.SeqEmpty
		} else {
			l.seq = numEntries - 1
		}
		return l.writeJournal()
	}
	if err != nil {
		return err
	}

	l.seq = int64(binary.BigEndian.Uint64(seqBuf[:]))
	return nil
}

func (l *Log[T]) writeJournal() error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(l.seq))
	_, err := l.jrnl.WriteAt(buf[:], 0)
	if err != nil {
		return err
	}
	return l.jrnl.Sync()
}

// Seq returns the current sequence number (index of last entry).
func (l *Log[T]) Seq() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.seq
}

// Append adds a value and returns its sequence number.
func (l *Log[T]) Append(value T) (int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return margaret.SeqErrored, ErrClosed
	}

	data, err := value.MarshalBinary()
	if err != nil {
		return margaret.SeqErrored, fmt.Errorf("offset2: marshal: %w", err)
	}

	// Get current data file position
	dataPos, err := l.data.Seek(0, io.SeekEnd)
	if err != nil {
		return margaret.SeqErrored, fmt.Errorf("offset2: seek data: %w", err)
	}

	// Write length-prefixed data
	var lenBuf [8]byte
	binary.BigEndian.PutUint64(lenBuf[:], uint64(len(data)))
	if _, err := l.data.Write(lenBuf[:]); err != nil {
		return margaret.SeqErrored, fmt.Errorf("offset2: write len: %w", err)
	}
	if _, err := l.data.Write(data); err != nil {
		return margaret.SeqErrored, fmt.Errorf("offset2: write data: %w", err)
	}

	// Write offset
	var ofstBuf [8]byte
	binary.BigEndian.PutUint64(ofstBuf[:], uint64(dataPos))
	if _, err := l.ofst.Seek(0, io.SeekEnd); err != nil {
		return margaret.SeqErrored, fmt.Errorf("offset2: seek ofst: %w", err)
	}
	if _, err := l.ofst.Write(ofstBuf[:]); err != nil {
		return margaret.SeqErrored, fmt.Errorf("offset2: write ofst: %w", err)
	}

	// Update sequence
	l.seq++
	if err := l.writeJournal(); err != nil {
		return margaret.SeqErrored, fmt.Errorf("offset2: write journal: %w", err)
	}

	// Sync
	if err := l.data.Sync(); err != nil {
		return margaret.SeqErrored, err
	}
	if err := l.ofst.Sync(); err != nil {
		return margaret.SeqErrored, err
	}

	newSeq := l.seq

	// Fire hooks (outside critical path, but still holding lock for consistency)
	l.hooksMu.RLock()
	hooks := make([]margaret.AppendHook[T], 0, len(l.hooks))
	for _, h := range l.hooks {
		hooks = append(hooks, h)
	}
	l.hooksMu.RUnlock()

	for _, h := range hooks {
		h(newSeq, value)
	}

	return newSeq, nil
}

// AppendBatch appends multiple values atomically with a single lock and fsync.
// The hook is fired once after the entire batch with the last entry's sequence
// and value, so live queries wake up once and drain all new entries.
// On write failure, the data and offset files are truncated to pre-batch state.
func (l *Log[T]) AppendBatch(values []T) ([]int64, error) {
	if len(values) == 0 {
		return nil, nil
	}

	// Pre-encode all values before taking the lock to fail fast on marshal errors.
	encoded := make([][]byte, len(values))
	for i, v := range values {
		data, err := v.MarshalBinary()
		if err != nil {
			return nil, fmt.Errorf("offset2: marshal entry %d: %w", i, err)
		}
		encoded[i] = data
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return nil, ErrClosed
	}

	// Record pre-batch positions for rollback.
	dataPos, err := l.data.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, fmt.Errorf("offset2: seek data: %w", err)
	}
	ofstPos, err := l.ofst.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, fmt.Errorf("offset2: seek ofst: %w", err)
	}
	origSeq := l.seq

	seqs := make([]int64, len(values))
	currentDataPos := dataPos

	for i, data := range encoded {
		// Write length-prefixed data.
		var lenBuf [8]byte
		binary.BigEndian.PutUint64(lenBuf[:], uint64(len(data)))
		if _, err := l.data.Write(lenBuf[:]); err != nil {
			l.rollback(dataPos, ofstPos, origSeq)
			return nil, fmt.Errorf("offset2: write len %d: %w", i, err)
		}
		if _, err := l.data.Write(data); err != nil {
			l.rollback(dataPos, ofstPos, origSeq)
			return nil, fmt.Errorf("offset2: write data %d: %w", i, err)
		}

		// Write offset entry.
		var ofstBuf [8]byte
		binary.BigEndian.PutUint64(ofstBuf[:], uint64(currentDataPos))
		if _, err := l.ofst.Write(ofstBuf[:]); err != nil {
			l.rollback(dataPos, ofstPos, origSeq)
			return nil, fmt.Errorf("offset2: write ofst %d: %w", i, err)
		}

		l.seq++
		seqs[i] = l.seq
		currentDataPos += 8 + int64(len(data))
	}

	// Single journal write + fsync for the whole batch.
	if err := l.writeJournal(); err != nil {
		l.rollback(dataPos, ofstPos, origSeq)
		return nil, fmt.Errorf("offset2: write journal: %w", err)
	}
	if err := l.data.Sync(); err != nil {
		l.rollback(dataPos, ofstPos, origSeq)
		return nil, fmt.Errorf("offset2: sync data: %w", err)
	}
	if err := l.ofst.Sync(); err != nil {
		// Data is already synced; best-effort rollback.
		l.rollback(dataPos, ofstPos, origSeq)
		return nil, fmt.Errorf("offset2: sync ofst: %w", err)
	}

	// Fire hooks once for the last entry (live queries drain by seq range).
	lastSeq := seqs[len(seqs)-1]
	lastVal := values[len(values)-1]

	l.hooksMu.RLock()
	hooks := make([]margaret.AppendHook[T], 0, len(l.hooks))
	for _, h := range l.hooks {
		hooks = append(hooks, h)
	}
	l.hooksMu.RUnlock()

	for _, h := range hooks {
		h(lastSeq, lastVal)
	}

	return seqs, nil
}

// rollback truncates data and offset files to their pre-batch positions
// and restores the sequence counter. Best-effort; errors are ignored
// because we are already in an error path.
func (l *Log[T]) rollback(dataPos, ofstPos int64, seq int64) {
	_ = l.data.Truncate(dataPos)
	_ = l.ofst.Truncate(ofstPos)
	l.seq = seq
}

// Get retrieves the value at seq.
func (l *Log[T]) Get(seq int64) (T, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var zero T

	if l.closed {
		return zero, ErrClosed
	}

	if seq < 0 || seq > l.seq {
		return zero, ErrSeqOutOfRange
	}

	data, err := l.readEntry(seq)
	if err != nil {
		return zero, err
	}

	if len(data) == 0 {
		return zero, ErrNulled
	}

	val := margaret.NewValue[T]()
	if err := val.UnmarshalBinary(data); err != nil {
		return zero, err
	}

	return val, nil
}

func (l *Log[T]) readEntry(seq int64) ([]byte, error) {
	// Read offset
	var ofstBuf [8]byte
	if _, err := l.ofst.ReadAt(ofstBuf[:], seq*8); err != nil {
		return nil, fmt.Errorf("offset2: read ofst: %w", err)
	}
	offset := int64(binary.BigEndian.Uint64(ofstBuf[:]))

	// Read length
	var lenBuf [8]byte
	if _, err := l.data.ReadAt(lenBuf[:], offset); err != nil {
		return nil, fmt.Errorf("offset2: read len: %w", err)
	}
	length := binary.BigEndian.Uint64(lenBuf[:])

	if length == 0 {
		return nil, nil // Nulled entry
	}

	// Read data
	data := make([]byte, length)
	if _, err := l.data.ReadAt(data, offset+8); err != nil {
		return nil, fmt.Errorf("offset2: read data: %w", err)
	}

	return data, nil
}

// Null zeroes out the entry at seq.
func (l *Log[T]) Null(seq int64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return ErrClosed
	}

	if seq < 0 || seq > l.seq {
		return ErrSeqOutOfRange
	}

	// Read current offset
	var ofstBuf [8]byte
	if _, err := l.ofst.ReadAt(ofstBuf[:], seq*8); err != nil {
		return fmt.Errorf("offset2: read ofst: %w", err)
	}
	offset := int64(binary.BigEndian.Uint64(ofstBuf[:]))

	// Read current length
	var lenBuf [8]byte
	if _, err := l.data.ReadAt(lenBuf[:], offset); err != nil {
		return fmt.Errorf("offset2: read len: %w", err)
	}
	length := binary.BigEndian.Uint64(lenBuf[:])

	// Zero out length prefix (marks as nulled)
	var zeroBuf [8]byte
	if _, err := l.data.WriteAt(zeroBuf[:], offset); err != nil {
		return fmt.Errorf("offset2: write zero len: %w", err)
	}

	// Zero out data
	zeros := make([]byte, length)
	if _, err := l.data.WriteAt(zeros, offset+8); err != nil {
		return fmt.Errorf("offset2: write zeros: %w", err)
	}

	return l.data.Sync()
}

// Replace overwrites the entry at seq with new data.
// The new data must not be larger than the existing entry.
func (l *Log[T]) Replace(seq int64, newData []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return ErrClosed
	}

	if seq < 0 || seq > l.seq {
		return ErrSeqOutOfRange
	}

	// Read current offset
	var ofstBuf [8]byte
	if _, err := l.ofst.ReadAt(ofstBuf[:], seq*8); err != nil {
		return fmt.Errorf("offset2: read ofst: %w", err)
	}
	offset := int64(binary.BigEndian.Uint64(ofstBuf[:]))

	// Read current length
	var lenBuf [8]byte
	if _, err := l.data.ReadAt(lenBuf[:], offset); err != nil {
		return fmt.Errorf("offset2: read len: %w", err)
	}
	currentLen := binary.BigEndian.Uint64(lenBuf[:])

	if uint64(len(newData)) > currentLen {
		return fmt.Errorf("offset2: new data (%d bytes) larger than slot (%d bytes)", len(newData), currentLen)
	}

	// Write new length
	binary.BigEndian.PutUint64(lenBuf[:], uint64(len(newData)))
	if _, err := l.data.WriteAt(lenBuf[:], offset); err != nil {
		return fmt.Errorf("offset2: write new len: %w", err)
	}

	// Write new data (padded with zeros if smaller)
	padded := make([]byte, currentLen)
	copy(padded, newData)
	if _, err := l.data.WriteAt(padded, offset+8); err != nil {
		return fmt.Errorf("offset2: write new data: %w", err)
	}

	return l.data.Sync()
}

// OnAppend registers a hook called after each append.
// Returns a function to unregister the hook.
func (l *Log[T]) OnAppend(hook margaret.AppendHook[T]) func() {
	l.hooksMu.Lock()
	id := l.hookID
	l.hookID++
	l.hooks[id] = hook
	l.hooksMu.Unlock()

	return func() {
		l.hooksMu.Lock()
		delete(l.hooks, id)
		l.hooksMu.Unlock()
	}
}

// Path returns the directory path of the log.
func (l *Log[T]) Path() string {
	return l.path
}

// Close closes the log files.
func (l *Log[T]) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return nil
	}
	l.closed = true

	var errs []error
	if err := l.data.Close(); err != nil {
		errs = append(errs, err)
	}
	if err := l.ofst.Close(); err != nil {
		errs = append(errs, err)
	}
	if err := l.jrnl.Close(); err != nil {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// Compile-time interface checks
type testValue int64

func (tv *testValue) UnmarshalBinary(data []byte) error {
	if len(data) != 8 {
		return fmt.Errorf("testValue: expected 8 bytes, got %d", len(data))
	}
	*tv = testValue(binary.BigEndian.Uint64(data))
	return nil
}

func (tv testValue) MarshalBinary() ([]byte, error) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(tv))
	return buf[:], nil
}

var (
	_ margaret.Encodeable                 = (*testValue)(nil)
	_ margaret.Log[*testValue]            = (*Log[*testValue])(nil)
	_ margaret.NullableLog[*testValue]    = (*Log[*testValue])(nil)
	_ margaret.ReplaceableLog[*testValue] = (*Log[*testValue])(nil)
	_ margaret.Alterable[*testValue]      = (*Log[*testValue])(nil)
	_ margaret.HookableLog[*testValue]    = (*Log[*testValue])(nil)
	_ margaret.BatchAppender[*testValue]  = (*Log[*testValue])(nil)
)
