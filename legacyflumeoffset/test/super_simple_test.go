// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

package test

import (
	"context"
	"os"
	"testing"

	"github.com/ssbc/margaret"
	lfo "github.com/ssbc/margaret/legacyflumeoffset"
	"github.com/stretchr/testify/require"
)

func TestShortExample(t *testing.T) {
	r := require.New(t)

	var tc testCodec

	const fname = "example.lfo"
	_, err := os.Stat(fname)
	if err != nil && os.IsNotExist(err) {
		t.Error(fname, "does not exist. use offsetexample.js to create it")
		return
	}
	r.NoError(err)

	log, err := lfo.Open(fname, tc)
	r.NoError(err)

	n := 6
	src, err := log.Query(margaret.Limit(n))
	r.NoError(err)

	// iterate all the entries
	ctx := context.TODO()
	for i := 0; i < n; i++ {
		v, err := src.Next(ctx)
		r.NoError(err, "error on entry %d", i)

		t.Logf("%s", v)
	}

	v, err := src.Next(ctx)
	r.Error(err)
	r.Nil(v)
}

func XTestShortFixtures(t *testing.T) {
	r := require.New(t)

	var tc testCodec

	// get the dinghy version of ssb-fixutres to test this out
	// didnt want to commit 8mb to this repo for this
	log, err := lfo.Open("fixtures.lfo", tc)
	r.NoError(err)

	n := 32

	src, err := log.Query(margaret.Limit(n))
	r.NoError(err)

	// iterate all the entries
	ctx := context.TODO()
	for i := 0; i < n; i++ {

		v, err := src.Next(ctx)
		r.NoError(err, "error on entry %d", i)

		b, ok := v.([]byte)
		r.True(ok, "v is not a byte slice?! %T", v)
		r.True(len(b) > 64, "short entry? %d", len(b))
		t.Logf("%s...", b[:64])
	}

	v, err := src.Next(ctx)
	r.Error(err)
	r.Nil(v)
}
