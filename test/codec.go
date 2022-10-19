// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

package test

import (
	"github.com/ssbc/margaret"
)

// NewCodecFunc is a function that returns a codec
type NewCodecFunc func(tipe interface{}) margaret.Codec
