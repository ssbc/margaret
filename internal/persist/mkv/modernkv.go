// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

package mkv

import (
	"fmt"
	"os"

	"modernc.org/kv"

	"github.com/ssbc/margaret/v2/internal/persist"
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
			return nil, fmt.Errorf("failed to create KV: %w", err)
		}
	} else if err != nil {
		return nil, fmt.Errorf("failed to stat path location: %w", err)
	} else {
		ms.db, err = kv.Open(path, opts)
		if err != nil {
			return nil, fmt.Errorf("failed to open KV %w", err)
		}
	}

	return &ms, nil
}
