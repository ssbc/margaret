// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"fmt"
	"os"

	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/sroar"
	"github.com/pkg/errors"
	"go.mindeco.de/logging"
)

var check = logging.CheckFatal

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "usage: %s <dir> (hasAddr)", os.Args[0])
		os.Exit(1)
	}
	logging.SetupLogging(nil)
	// log := logging.Logger(os.Args[0])

	dir := os.Args[1]

	opts := badger.DefaultOptions(dir)

	db, err := badger.Open(opts)
	check(errors.Wrap(err, "error opening database"))

	err = db.View(func(txn *badger.Txn) error {

		opts := badger.DefaultIteratorOptions
		iter := txn.NewIterator(opts)
		for iter.Rewind(); iter.Valid(); iter.Next() {
			it := iter.Item()
			k := it.Key()

			var dataLen int
			var debugData string
			err = it.Value(func(v []byte) error {
				dataLen = len(v)
				if bytes.HasPrefix(k, []byte("mlog-")) {
					bmap := sroar.FromBuffer(v)
					debugData = bmap.String()
				} else {
					debugData = string(v)
				}
				return nil
			})
			check(err)

			fmt.Printf("%q: %d\n", string(k), dataLen)
			fmt.Println(debugData + "\n")

		}
		iter.Close()

		return nil
	})
	check(err)

	check(db.Close())
	// // check has
	// if len(os.Args) > 2 {
	// 	addr := indexes.Addr(os.Args[2])
	// 	has, err := multilog.Has(mlog, addr)
	// 	log.Log("mlog", "has", "addr", addr, "has?", has, "hasErr", err)
	// }
}
