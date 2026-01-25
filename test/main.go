// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

package test

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/ssbc/margaret/v2"
)

type Entry int64

func (te Entry) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer
	err := binary.Write(&buf, binary.BigEndian, int64(te))
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (te *Entry) UnmarshalBinary(data []byte) error {
	var i int64
	err := binary.Read(bytes.NewReader(data), binary.BigEndian, &i)
	if err != nil {
		return err
	}
	*te = Entry(i)
	return nil
}

type Log = margaret.Log[*Entry]

type NewLogFunc func(string) (Log, error)

func LogTest(f NewLogFunc) func(*testing.T) {
	return func(t *testing.T) {
		t.Run("Get", LogTestGet(f))
		t.Run("Simple", LogTestSimple(f))
		t.Run("Concurrent", LogTestConcurrent(f))
	}
}
