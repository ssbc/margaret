// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

package offset2

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReplace(t *testing.T) {
	os.RemoveAll("testrun")
	tevs := []testEvent{
		{"hello", 23},
		{"world", 42},
		{"world", 161},
		{"world", 1312},
		{"moar", 1234},
		{strings.Repeat("ACAB", 191), 1312},
		{"s", 1}, // small
	}

	for i := 0; i < len(tevs); i++ {
		var seq = int64(i)
		t.Run(strconv.Itoa(i), replaceOne(tevs, seq))
	}
}

func replaceOne(tevs []testEvent, replSeq int64) func(*testing.T) {
	return func(t *testing.T) {
		r := require.New(t)
		a := assert.New(t)

		name := filepath.Join("testrun", t.Name())

		log, err := Open[*testEvent](name)
		r.NoError(err, "error during log creation")

		for i, ev := range tevs {
			seq, err := log.Append(&ev)
			r.NoError(err, "failed to append event %d", i)
			r.EqualValues(i, seq, "sequence mismatch")
		}

		repEvt := testEvent{"R", 0}
		replaceData, err := json.Marshal(repEvt)
		r.NoError(err)

		// reopen for const check
		r.NoError(log.Close())
		log, err = Open[*testEvent](name)
		r.NoError(err, "error reopening log")

		seq := log.Seq()
		r.EqualValues(len(tevs)-1, seq, "sequence mismatch")

		err = log.Replace(replSeq, replaceData)
		r.NoError(err, "failed to replace")
		r.NoError(log.Close())

		// reopen after replace
		log, err = Open[*testEvent](name)
		r.NoError(err, "error reopening log #2")

		// get loop - verify replace
		for i := 0; i < len(tevs); i++ {
			te, err := log.Get(int64(i))
			r.NoError(err, "error reading from log")
			if i == int(replSeq) {
				a.Equal(repEvt, *te)
			} else {
				a.Equal(tevs[i], *te)
			}
		}

		// iter drain - verify replace
		qry := log.Query()
		i := 0
		for _, te := range qry.Iter() {
			if i == int(replSeq) {
				a.Equal(repEvt, *te)
			} else {
				a.Equal(tevs[i], *te)
			}
			i++
		}
		r.NoError(qry.Err())
		r.Equal(len(tevs), i)
	}
}
