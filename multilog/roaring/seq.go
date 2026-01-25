// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

package roaring

import (
	"encoding/binary"
	"fmt"
)

// Seq is a sequence number reference stored in a roaring bitmap sublog.
// It wraps an int64 representing a position in a source log.
type Seq int64

func (s *Seq) MarshalBinary() ([]byte, error) {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(*s))
	return buf, nil
}

func (s *Seq) UnmarshalBinary(data []byte) error {
	if len(data) != 8 {
		return fmt.Errorf("roaring.Seq: expected 8 bytes, got %d", len(data))
	}
	*s = Seq(binary.BigEndian.Uint64(data))
	return nil
}
