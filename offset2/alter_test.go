// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

package offset2

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNull(t *testing.T) {
	os.RemoveAll("testrun")
	tevs := []testEvent{
		{"hello", 23},
		{"world", 42},
		{"world", 161},
		{"world", 1312},
		{"moar", 1234},
	}

	for i := 0; i < len(tevs); i++ {
		var seq = int64(i)
		t.Run(strconv.Itoa(i), nullOne(tevs, seq))
	}
}

func nullOne(tevs []testEvent, nullSeq int64) func(*testing.T) {
	return func(t *testing.T) {
		r := require.New(t)
		a := assert.New(t)

		name := filepath.Join("testrun", t.Name())

		log, err := Open[*testEvent](name)
		r.NoError(err, "error during log creation")

		// fill
		for i, ev := range tevs {
			seq, err := log.Append(&ev)
			r.NoError(err, "failed to append event %d", i)
			r.Equal(int64(i), seq, "sequence mismatch")
		}

		// reopen for const check
		r.NoError(log.Close())
		log, err = Open[*testEvent](name)
		r.NoError(err, "error reopening log")

		seq := log.Seq()
		r.EqualValues(int64(len(tevs)-1), seq, "sequence mismatch")

		err = log.Null(nullSeq)
		r.NoError(err, "failed null")

		// make sure we can null twice without an error
		err = log.Null(nullSeq)
		r.NoError(err, "failed null (again)")

		// reopen after null
		r.NoError(log.Close())
		log, err = Open[*testEvent](name)
		r.NoError(err, "error reopening log #2")

		// get loop - verify null
		for i := 0; i < len(tevs); i++ {
			v, err := log.Get(int64(i))
			if i == int(nullSeq) {
				r.True(errors.Is(err, ErrNulled))
				r.Nil(v)
			} else {
				r.NoError(err, "error reading from log")
				a.Equal(tevs[i], *v)
			}
		}

		// iter drain - nulled entries are skipped
		qry := log.Query()
		count := 0
		for seq, val := range qry.Iter() {
			r.NotEqual(nullSeq, seq, "nulled entry should be skipped")
			a.Equal(tevs[seq], *val)
			count++
		}
		r.NoError(qry.Err())
		r.Equal(len(tevs)-1, count, "should have one fewer entry due to null")
	}
}
