// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

package sqlite

import (
	"github.com/ssbc/margaret/internal/persist/sqlite"
	"github.com/ssbc/margaret/multilog/roaring"
)

func NewMultiLog(base string) (*roaring.MultiLog, error) {
	s, err := sqlite.New(base)
	if err != nil {
		return nil, err
	}
	return roaring.NewStore(s), nil
}
