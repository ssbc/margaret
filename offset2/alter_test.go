// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

package offset2

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.cryptoscope.co/luigi"
	"go.cryptoscope.co/margaret"
	mjson "go.cryptoscope.co/margaret/codec/json"
)

var _ margaret.Alterer = (*OffsetLog)(nil)

func TestNull(t *testing.T) {
	os.RemoveAll("testrun")
	tevs := []testEvent{
		testEvent{"hello", 23},
		testEvent{"world", 42},
		testEvent{"world", 161},
		testEvent{"world", 1312},
		testEvent{"moar", 1234},
	}

	for i := 0; i < len(tevs); i++ {
		var seq = int64(i)
		t.Run(strconv.Itoa(i), nullOne(tevs, seq))
	}
}

func nullOne(tevs []testEvent, nullSeq int64) func(*testing.T) {
	return func(t *testing.T) {
		//setup
		r := require.New(t)
		a := assert.New(t)

		name := filepath.Join("testrun", t.Name())

		log, err := Open(name, mjson.New(&testEvent{}))
		r.NoError(err, "error during log creation")

		// fill
		for i, ev := range tevs {
			seq, err := log.Append(ev)
			r.NoError(err, "failed to append event %d", i)
			r.Equal(int64(i), seq, "sequence missmatch")
		}

		r.NoError(log.Close())

		// reopen for const check
		log, err = Open(name, mjson.New(&testEvent{}))
		r.NoError(err, "error reopening log")

		seq := log.Seq()
		r.EqualValues(int64(len(tevs)-1), seq, "sequence missmatch")

		err = log.Null(nullSeq)
		r.NoError(err, "failed null")

		// make sure we can null twice without an error
		err = log.Null(nullSeq)
		r.NoError(err, "failed null (again)")

		// reopen after null
		r.NoError(log.Close())
		log, err = Open(name, mjson.New(&testEvent{}))
		r.NoError(err, "error reopening log #2")

		// get loop
		for i := 0; i < len(tevs); i++ {
			v, err := log.Get(int64(i))
			if i == int(nullSeq) {
				r.True(errors.Is(err, margaret.ErrNulled))
				r.True(margaret.IsErrNulled(err))
				r.Nil(v)
			} else {
				r.NoError(err, "error reopening log")
				te, ok := v.(*testEvent)
				r.True(ok)
				a.Equal(tevs[i], *te)
			}
		}

		// pump drain
		ctx := context.TODO()
		src, err := log.Query()
		r.NoError(err)

		i := 0
		snk := luigi.FuncSink(func(ctx context.Context, v interface{}, err error) error {
			if err != nil {
				if luigi.IsEOS(err) {
					return nil
				}
				return err
			}
			if i == int(nullSeq) {
				r.Equal(margaret.ErrNulled, v)
			}
			i++
			return nil
		})

		err = luigi.Pump(ctx, snk, src)
		r.NoError(err)
		r.Equal(len(tevs), i)

		// manual drain
		src, err = log.Query()
		r.NoError(err)

		i = 0
		for {
			v, err := src.Next(ctx)
			// fmt.Println(i, v, err)
			if luigi.IsEOS(err) {
				break
			}
			if i == int(nullSeq) {
				a.Equal(margaret.ErrNulled, v)
			}
			i++
		}
		r.Equal(len(tevs), i)
	}
}
