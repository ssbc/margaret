// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

package badger

import (
	"github.com/dgraph-io/badger/v3"

	pbadger "github.com/ssbc/margaret/internal/persist/badger"
	"github.com/ssbc/margaret/multilog/roaring"
)

func NewStandalone(base string) (*roaring.MultiLog, error) {
	s, err := pbadger.NewStandalone(base)
	if err != nil {
		return nil, err
	}
	return roaring.NewStore(s), nil
}

func NewShared(db *badger.DB, keyPrefix []byte) (*roaring.MultiLog, error) {
	s, err := pbadger.NewShared(db, keyPrefix)
	if err != nil {
		return nil, err
	}
	return roaring.NewStore(s), nil
}
