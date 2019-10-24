package offset2

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.cryptoscope.co/luigi"
	"go.cryptoscope.co/margaret"
	mjson "go.cryptoscope.co/margaret/codec/json"
)

func TestReplace(t *testing.T) {
	os.RemoveAll("testrun")
	tevs := []testEvent{
		testEvent{"hello", 23},
		testEvent{"world", 42},
		testEvent{"world", 161},
		testEvent{"world", 1312},
		testEvent{"moar", 1234},
		testEvent{strings.Repeat("ACAB", 191), 1312},
		testEvent{"s", 1}, // small
	}

	for i := 0; i < len(tevs); i++ {
		var seq = margaret.BaseSeq(i)
		t.Run(strconv.Itoa(i), replaceOne(tevs, seq))
	}
}

func replaceOne(tevs []testEvent, nullSeq margaret.Seq) func(*testing.T) {
	return func(t *testing.T) {
		//setup
		r := require.New(t)
		a := assert.New(t)

		name := filepath.Join("testrun", t.Name())

		log, err := Open(name, mjson.New(&testEvent{}))
		r.NoError(err, "error during log creation")

		for i, ev := range tevs {
			seq, err := log.Append(ev)
			r.NoError(err, "failed to append event %d", i)
			r.Equal(margaret.BaseSeq(i), seq, "sequence missmatch")
		}

		repEvt := testEvent{"REPLACE", 0}
		replaceData, err := json.Marshal(repEvt)
		r.NoError(err)

		// reopen for const check
		r.NoError(log.Close())
		log, err = Open(name, mjson.New(&testEvent{}))
		r.NoError(err, "error reopening log")

		seq, err := log.Seq().Value()
		r.NoError(err, "failed get current value")
		r.Equal(margaret.BaseSeq(len(tevs)-1), seq, "sequence missmatch")

		err = log.Replace(nullSeq, replaceData)
		r.NoError(err, "failed get current value")
		r.NoError(log.Close())

		// reopen after null
		log, err = Open(name, mjson.New(&testEvent{}))
		r.NoError(err, "error reopening log #2")

		// get loop
		for i := 0; i < len(tevs); i++ {
			v, err := log.Get(margaret.BaseSeq(i))
			r.NoError(err, "error reading from log")
			te, ok := v.(*testEvent)
			r.True(ok, "wrong type: %T %v", v, v)
			if int64(i) == nullSeq.Seq() {
				a.Equal(repEvt, *te)
			} else {
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
			te, ok := v.(*testEvent)
			r.True(ok, "wrong type: %T %v", v, v)
			if int64(i) == nullSeq.Seq() {
				a.Equal(repEvt, *te)
			} else {
				a.Equal(tevs[i], *te)
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
			if luigi.IsEOS(errors.Cause(err)) {
				break
			}
			te, ok := v.(*testEvent)
			r.True(ok, "wrong type: %T %v", v, v)
			if int64(i) == nullSeq.Seq() {
				a.Equal(repEvt, *te)
			} else {
				a.Equal(tevs[i], *te)
			}
			i++
		}
		r.Equal(len(tevs), i)
	}
}
