// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

package test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFactor(t *testing.T) {
	type testcase struct {
		n       int
		factors []int
	}

	test := func(tc testcase) func(*testing.T) {
		return func(t *testing.T) {
			a := assert.New(t)
			out := factorize(tc.n)
			a.Equal(tc.factors, out, "factor mismatch")
		}
	}

	tcs := []testcase{
		{
			n:       30,
			factors: []int{2, 3, 5},
		},
		{
			n:       50,
			factors: []int{2, 5, 5},
		},
		{
			n:       1024,
			factors: []int{2, 2, 2, 2, 2, 2, 2, 2, 2, 2},
		},
	}

	for i, tc := range tcs {
		t.Run(fmt.Sprint(i), test(tc))
	}
}
