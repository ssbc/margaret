// SPDX-License-Identifier: MIT

package remote

import (
	"context"

	"go.cryptoscope.co/luigi"

	"go.cryptoscope.co/librarian"
)

// MsgType is either "set" or "delete"
type MsgType string

// Message is a data type that is passed through a source to set or delelte values.
type Message struct {
	Type  MsgType
	Addr  librarian.Addr
	Value interface{}
}

// IndexerFunc is a function that processes the values read from
// the source and updates an Index.
type IndexerFunc func(context.Context, luigi.Source, librarian.Index) error
