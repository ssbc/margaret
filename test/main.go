// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

package test // import "github.com/ssbc/margaret/test"

import (
	"testing"

	"github.com/ssbc/margaret"
)

type NewLogFunc func(string, interface{}) (margaret.Log, error)

func LogTest(f NewLogFunc) func(*testing.T) {
	return func(t *testing.T) {
		t.Run("Get", LogTestGet(f))
		t.Run("Simple", LogTestSimple(f))
		t.Run("Concurrent", LogTestConcurrent(f))
	}
}
