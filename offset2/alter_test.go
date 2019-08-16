package offset2

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.cryptoscope.co/luigi"
	"go.cryptoscope.co/margaret"
	mjson "go.cryptoscope.co/margaret/codec/json"
)

var _ margaret.Alterer = (*offsetLog)(nil)

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
		var seq = margaret.BaseSeq(i)
		t.Run(strconv.Itoa(i), nullOne(tevs, seq))
	}
}

func nullOne(tevs []testEvent, nullSeq margaret.Seq) func(*testing.T) {
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
			r.Equal(margaret.BaseSeq(i), seq, "sequence missmatch")
		}

		r.NoError(log.Close())

		// reopen for const check
		log, err = Open(name, mjson.New(&testEvent{}))
		r.NoError(err, "error reopening log")

		seq, err := log.Seq().Value()
		r.NoError(err, "failed get current value")
		r.Equal(margaret.BaseSeq(len(tevs)-1), seq, "sequence missmatch")

		err = log.Null(nullSeq)
		r.NoError(err, "failed get current value")
		r.NoError(log.Close())

		// reopen after null
		log, err = Open(name, mjson.New(&testEvent{}))
		r.NoError(err, "error reopening log #2")

		// get loop
		for i := 0; i < len(tevs); i++ {
			v, err := log.Get(margaret.BaseSeq(i))
			if int64(i) == nullSeq.Seq() {
				// r.Error(err)
				r.EqualError(margaret.ErrNulled, errors.Cause(err).Error())
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
			if int64(i) == nullSeq.Seq() {
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
			if luigi.IsEOS(errors.Cause(err)) {
				break
			}
			if int64(i) == nullSeq.Seq() {
				a.Equal(margaret.ErrNulled, v)
			}
			i++
		}
		r.Equal(len(tevs), i)
	}
}
