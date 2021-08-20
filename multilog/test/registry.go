// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

package test

import (
	"testing"
)

var NewLogFuncs map[string]NewLogFunc

func init() {
	NewLogFuncs = map[string]NewLogFunc{}
}

func Register(name string, f NewLogFunc) {
	NewLogFuncs[name] = f
}

func RunSinkTests(t *testing.T) {
	if len(NewLogFuncs) == 0 {
		t.Fatal("found no multilogs")
	}
	for name, newLog := range NewLogFuncs {
		t.Run(name, SinkTest(newLog))
	}
}

func RunMultiLogTests(t *testing.T) {
	if len(NewLogFuncs) == 0 {
		t.Fatal("found no multilogs")
	}
	for name, newLog := range NewLogFuncs {
		t.Run(name+"-basic", MultiLogTest(newLog))
		t.Run(name+"-handwoven", MultilogTestAddLogAndListed(newLog))
	}
}

func RunSubLogTests(t *testing.T) {
	if len(NewLogFuncs) == 0 {
		t.Fatal("found no multilogs")
	}
	for name, newLog := range NewLogFuncs {
		t.Run(name, SubLogTest(newLog))
	}
}
