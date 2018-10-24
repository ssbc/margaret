package offset2

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"go.cryptoscope.co/margaret"
	mjson "go.cryptoscope.co/margaret/codec/json"
)

type testEvent struct {
	Foo string
	Bar int
}

func TestReadWrite(t *testing.T) {
	//setup
	r := require.New(t)
	name, err := ioutil.TempDir("", t.Name())
	r.NoError(err)

	log, err := Open(name, mjson.New(&testEvent{}))
	r.NoError(err, "error during log creation")

	// fill
	tevs := []testEvent{
		testEvent{"hello", 23},
		testEvent{"world", 42},
		testEvent{"world", 161},
		testEvent{"world", 1312},
	}
	for i, ev := range tevs {
		seq, err := log.Append(ev)
		r.NoError(err, "failed to append event %d", i)
		r.Equal(margaret.BaseSeq(i), seq, "sequence missmatch")
	}

	// read
	for i := 0; i < len(tevs); i++ {
		v, err := log.Get(margaret.BaseSeq(i))
		r.NoError(err, "failed to get event %d", i)

		ev, ok := v.(*testEvent)
		r.True(ok, "failed to cast event %d. got %T", i, v)
		r.Equal(*ev, tevs[i])
	}

	// cleanup
	if t.Failed() {
		t.Logf("log data directory at %q was not deleted due to test failure", name)
	} else {
		os.RemoveAll(name)
	}
}

// make sure that the sequence is picked up after opening an existing log
func TestWriteAndWriteAgain(t *testing.T) {
	//setup
	r := require.New(t)
	name, err := ioutil.TempDir("", t.Name())
	r.NoError(err)

	log, err := Open(name, mjson.New(&testEvent{}))
	r.NoError(err, "error during log creation")

	// fill
	tevs := []testEvent{
		testEvent{"hello", 23},
		testEvent{"world", 42},
		testEvent{"world", 161},
		testEvent{"world", 1312},
	}
	for i, ev := range tevs {
		seq, err := log.Append(ev)
		r.NoError(err, "failed to append event %d", i)
		r.Equal(margaret.BaseSeq(i), seq, "sequence missmatch")
	}

	log, err = Open(name, mjson.New(&testEvent{}))
	r.NoError(err, "error during log creation")

	// fill again
	for i, ev := range tevs {
		seq, err := log.Append(ev)
		r.NoError(err, "failed to do 2nd append %d", i)
		r.Equal(margaret.BaseSeq(len(tevs)+i), seq, "sequence missmatch %d", i)
	}

	// close
	log, err = Open(name, mjson.New(&testEvent{}))
	r.NoError(err, "error during log creation")

	currSeq, err := log.Seq().Value()
	r.NoError(err, "failed to get current sequence")
	r.Equal(margaret.BaseSeq(2*len(tevs)-1), currSeq.(margaret.BaseSeq))

	// read by seq
	for i := 0; i < 2*len(tevs); i++ {
		v, err := log.Get(margaret.BaseSeq(i))
		r.NoError(err, "failed to get event %d", i)

		ev, ok := v.(*testEvent)
		r.True(ok, "failed to cast event %d. got %T", i, v)
		r.Equal(*ev, tevs[i%len(tevs)])
	}

	/* qry
	src, err := log.Query()
	r.NoError(err, "failed to open query")
	var (
		ctx = context.TODO()
		seq int64
	)
	for {
		v, err := src.Next(ctx)
		if luigi.IsEOS(err) {
			break
		} else if err != nil {
			r.NoError(err, "error during next draining")
		}

		// TODO: v has no sequence unless we put it in the values ourselvs..?
	}
	*/

	// cleanup
	if t.Failed() {
		t.Log("log was written to ", name)
	} else {
		os.RemoveAll(name)
	}
}
