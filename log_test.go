// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

package margaret

import "testing"

var _ error = ErrNulled

func TestNulledErr(t *testing.T) {

	var e = ErrNulled

	var hidden interface{}

	hidden = e

	err, ok := hidden.(error)
	if !ok {
		t.Fatal("not an error")
	}

	if !IsErrNulled(err) {
		t.Fatal("not a nulled err")
	}
}
