// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

package persist

import (
	"errors"
	"fmt"
	"io"
)

type Key []byte

func (k Key) String() string {
	return fmt.Sprintf("key:%x", []byte(k))
}

var ErrNotFound = errors.New("persist: item not found")

type Saver interface {
	io.Closer
	Put(Key, []byte) error
	PutMultiple([]KeyValuePair) error

	Get(Key) ([]byte, error)

	List() ([]Key, error)

	Delete(Key) error
}

type KeyValuePair struct {
	Key   Key
	Value []byte
}
