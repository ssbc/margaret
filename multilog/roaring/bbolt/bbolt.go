// SPDX-FileCopyrightText: 2024 The margaret Authors
//
// SPDX-License-Identifier: MIT

// Package bbolt provides a bbolt-backed roaring bitmap multilog.
// This is well suited for sparse multilogs with many sublogs containing
// few entries each (e.g. tangles, mentions) where the filesystem backend
// would create thousands of tiny files.
package bbolt

import (
	bolt "go.etcd.io/bbolt"

	persistbbolt "github.com/ssbc/margaret/v2/internal/persist/bbolt"
	"github.com/ssbc/margaret/v2/multilog/roaring"
)

// NewMultiLog creates a roaring bitmap multilog backed by a new bbolt DB
// at path/bolt.db. The multilog owns the database and will close it when
// the multilog is closed.
func NewMultiLog(path string) (*roaring.MultiLog, error) {
	saver, err := persistbbolt.New(path)
	if err != nil {
		return nil, err
	}
	return roaring.NewStore(saver), nil
}

// NewMultiLogWithDB creates a roaring bitmap multilog backed by an existing
// bbolt DB, using the given bucket name for namespace isolation. Multiple
// multilogs can share the same DB by using different bucket names.
// The caller is responsible for closing the underlying DB after all
// multilogs using it have been closed.
func NewMultiLogWithDB(db *bolt.DB, bucket string) (*roaring.MultiLog, error) {
	saver, err := persistbbolt.NewWithDB(db, bucket)
	if err != nil {
		return nil, err
	}
	return roaring.NewStore(saver), nil
}
