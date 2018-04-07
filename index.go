package librarian // import "cryptoscope.co/go/librarian"

import (
	"context"

	"cryptoscope.co/go/luigi"
)

// Addr is an address (or key) in the index.
// TODO maybe not use a string but a Stringer or
// interface{ Addr() string }?
type Addr string

// Index provides an index table keyed by Addr that can be fed with
// a stream
type Index interface {
	luigi.Sink

	// Get returns the an observable of the value stored at the address.
	// Getting an unset value retuns a valid Observable with a value
	// of type Unset and a nil error.
	Get(context.Context, Addr) (luigi.Observable, error)
}

// Unset is the value of observable returned by idx.Get() when the
// requested address has not been set yet.
type Unset struct {
	Addr Addr
}

// Setter is passed to the function managing an index, which uses it
// to modify it.
//
// TODO maybe provide other index builders as well, e.g. for managing
// sets: add and remove values from and to sets, stored at address
type Setter interface {
	// Set sets a value in the index
	Set(context.Context, Addr, interface{}) error

	// Delete deletes a value from the index
	Delete(context.Context, Addr) error
}

// IndexSetterFunc is a function that processes the values read from
// the source and updates an index using Setter.
type IndexSetterFunc func(context.Context, luigi.Source, Setter) error
