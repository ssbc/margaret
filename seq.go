// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

package margaret // import "github.com/ssbc/margaret"

const (
	// SeqEmpty is the current sequence number of an empty log
	SeqEmpty int64 = -1

	// SeqErrored is returned if an operation (like Append) fails
	SeqErrored int64 = -2

	SeqSublogDeleted int64 = -255
)

// Seqer returns the current sequence of a log
type Seqer interface {
	Seq() int64
}
