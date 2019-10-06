// SPDX-License-Identifier: MIT

package test

import "testing"

var NewLogFuncs map[string]NewLogFunc

func init() {
	NewLogFuncs = map[string]NewLogFunc{}
}

func Register(name string, f NewLogFunc) {
	NewLogFuncs[name] = f
}

func RunTests(t *testing.T) {
	for name, newLog := range NewLogFuncs {
		t.Run(name, LogTest(newLog))
	}
}
