// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

package all

import (
	"testing"

	mltest "github.com/ssbc/margaret/multilog/test"
)

func TestSink(t *testing.T) {
	mltest.RunSinkTests(t)
}

func TestMultiLog(t *testing.T) {
	mltest.RunMultiLogTests(t)
}

func TestSubLog(t *testing.T) {
	mltest.RunSubLogTests(t)
}
