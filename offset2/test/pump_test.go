// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

package test

import (
	"testing"
)

func TestPump(t *testing.T) {
	for k, v := range newLogFuncs {
		t.Run(k+"/pump", LogTestPump(v))
		t.Run(k+"/pumplive", LogTestPumpLive(v))
	}
}
