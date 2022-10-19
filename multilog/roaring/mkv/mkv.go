// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

package mkv

import (
	"github.com/ssbc/margaret/internal/persist/mkv"
	"github.com/ssbc/margaret/multilog/roaring"
)

func NewMultiLog(base string) (*roaring.MultiLog, error) {
	s, err := mkv.New(base)
	if err != nil {
		return nil, err
	}
	return roaring.NewStore(s), nil
}
