// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

// Package fs provides a filesystem-backed roaring bitmap multilog.
package fs

import (
	"github.com/ssbc/margaret/v2/internal/persist/fs"
	"github.com/ssbc/margaret/v2/multilog/roaring"
)

// NewMultiLog creates a new roaring bitmap multilog backed by files at base.
func NewMultiLog(base string) *roaring.MultiLog {
	return roaring.NewStore(fs.New(base))
}
