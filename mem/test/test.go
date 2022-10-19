// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

package test

import (
	"github.com/ssbc/margaret"
	"github.com/ssbc/margaret/mem"
	mtest "github.com/ssbc/margaret/test"
)

func init() {
	mtest.Register("mem", func(string, interface{}) (margaret.Log, error) {
		return mem.New(), nil
	})
}
