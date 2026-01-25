// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT
// Package margaret provides a generic append-only log implementation.
package margaret

import (
	"encoding"
	"errors"
	"io"
	"iter"
	"reflect"
)

type Encodeable interface {
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
}

var (
	ErrOutOfBounds = errors.New("margaret: out-of-bounds access")
	ErrNulled      = errors.New("margaret: Entry Nulled")
)

// IsErrNulled returns true if the error indicates a nulled entry.
func IsErrNulled(err error) bool {
	return errors.Is(err, ErrNulled)
}

// NewValue creates a new instance of T, properly handling pointer types.
// For pointer types like *Foo, it allocates a new Foo and returns *Foo.
// For value types like Foo, it returns the zero value.
func NewValue[T any]() T {
	t := reflect.TypeOf((*T)(nil)).Elem()
	if t.Kind() == reflect.Pointer {
		return reflect.New(t.Elem()).Interface().(T)
	}
	var zero T
	return zero
}

// Log is a generic append-only log.
type Log[T Encodeable] interface {
	// Seq returns the current sequence number (latest entry index).
	// Returns SeqEmpty (-1) if the log is empty.
	Seq() int64

	// Append adds a value to the log and returns its sequence number.
	Append(T) (int64, error)

	// Get retrieves the value at the given sequence number.
	Get(seq int64) (T, error)

	// Query returns an iterator over log entries matching the query options.
	Query(...QueryOption) QueryIterator[T]

	io.Closer
}

type QueryIterator[T any] interface {
	Iter() iter.Seq2[int64, T]

	// Err should be checked after draining the iterator
	Err() error
}

type iteratorWrapper[T Encodeable] struct {
	iter  func(yield func(int64, T) bool)
	err   error
	errFn func() error // lazy error for live iterators
}

func (oi iteratorWrapper[T]) Err() error {
	if oi.errFn != nil {
		return oi.errFn()
	}
	return oi.err
}

func (oi iteratorWrapper[T]) Iter() iter.Seq2[int64, T] {
	return oi.iter
}

func NewIterWrapper[T Encodeable](yield func(func(int64, T) bool)) iteratorWrapper[T] {
	return iteratorWrapper[T]{
		iter: yield,
	}
}

// NewLiveIterWrapper creates an iterator whose error is determined after iteration completes.
func NewLiveIterWrapper[T Encodeable](yield func(func(int64, T) bool), errFn func() error) iteratorWrapper[T] {
	return iteratorWrapper[T]{
		iter:  yield,
		errFn: errFn,
	}
}

func NewFailedIterator[T Encodeable](err error) iteratorWrapper[T] {
	return iteratorWrapper[T]{
		err: err,
	}
}

// NullableLog extends Log with the ability to null (zero out) entries.
type NullableLog[T Encodeable] interface {
	Log[T]
	// Null zeroes out the entry at seq. The entry still exists but contains no data.
	Null(seq int64) error
}

// ReplaceableLog extends Log with the ability to replace entries.
type ReplaceableLog[T Encodeable] interface {
	Log[T]
	// Replace overwrites the entry at seq. New data must fit in existing space.
	Replace(seq int64, data []byte) error
}

// Alterable combines nullable and replaceable capabilities.
type Alterable[T Encodeable] interface {
	NullableLog[T]
	ReplaceableLog[T]
}

// AppendHook is called after a successful append.
// Useful for building live subscriptions on top.
type AppendHook[T Encodeable] func(seq int64, value T)

// HookableLog supports registering append hooks.
type HookableLog[T Encodeable] interface {
	Log[T]
	// OnAppend registers a hook called after each append.
	// Returns a function to unregister the hook.
	OnAppend(AppendHook[T]) (unregister func())
}
