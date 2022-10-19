// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

package fs

import (
	"github.com/ssbc/margaret/internal/persist/fs"
	"github.com/ssbc/margaret/multilog/roaring"
)

func NewMultiLog(base string) (*roaring.MultiLog, error) {
	return roaring.NewStore(fs.New(base)), nil
}
