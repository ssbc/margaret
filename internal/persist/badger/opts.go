// SPDX-FileCopyrightText: 2022 The margaret Authors
//
// SPDX-License-Identifier: MIT

//go:build !lite
// +build !lite

package badger

import (
	"github.com/dgraph-io/badger/v3"
)

func BadgerOpts(dbPath string) badger.Options {
	opts := badger.DefaultOptions(dbPath)
	return opts
}
