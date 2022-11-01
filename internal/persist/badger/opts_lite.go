// SPDX-FileCopyrightText: 2022 The margaret Authors
//
// SPDX-License-Identifier: MIT

//go:build lite
// +build lite

package badger

import (
	"github.com/dgraph-io/badger/v3"
)

func BadgerOpts(dbPath string) badger.Options {
	return badger.DefaultOptions(dbPath).
		WithMemTableSize(1 << 25).
		WithValueLogFileSize(1 << 25).
		WithNumMemtables(10).
		WithNumLevelZeroTables(3).
		WithNumLevelZeroTablesStall(7).
		WithNumCompactors(2).
		WithIndexCacheSize(1 << 27).
		WithBlockCacheSize(1 << 27).
		WithLogger(nil)
}
