// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

package test

import (
	"go.cryptoscope.co/margaret"
	"go.cryptoscope.co/margaret/mem"
	mtest "go.cryptoscope.co/margaret/test"
)

func init() {
	mtest.Register("mem", func(string, interface{}) (margaret.Log, error) {
		return mem.New(), nil
	})
}
