// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

package mkv

import (
	"os"

	"github.com/pkg/errors"
	"github.com/ssbc/margaret/internal/persist"
	"modernc.org/kv"
)

type ModernSaver struct {
	db *kv.DB
}

var _ persist.Saver = (*ModernSaver)(nil)

func (sl ModernSaver) Close() error {
	return sl.db.Close()
}

func New(path string) (*ModernSaver, error) {
	var ms ModernSaver

	opts := &kv.Options{}
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		ms.db, err = kv.Create(path, opts)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create KV")
		}
	} else if err != nil {
		return nil, errors.Wrap(err, "failed to stat path location")
	} else {
		ms.db, err = kv.Open(path, opts)
		if err != nil {
			return nil, errors.Wrap(err, "failed to open KV")
		}
	}

	return &ms, nil
}
