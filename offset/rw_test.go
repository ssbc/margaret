package offset

import (
	"io/ioutil"
	"os"
	"testing"

	"cryptoscope.co/go/margaret"
	mjson "cryptoscope.co/go/margaret/codec/json"
	"cryptoscope.co/go/margaret/framing/lengthprefixed"
	"github.com/stretchr/testify/require"
)

type testEvent struct {
	Foo string
	Bar int
}

func TestReadWrite(t *testing.T) {
	//setup
	r := require.New(t)
	f, err := ioutil.TempFile("", t.Name())
	r.NoError(err)

	log, err := NewOffsetLog(f, lengthprefixed.New32(4096), mjson.New(&testEvent{}))
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
		r.Equal(margaret.Seq(i), seq, "sequence missmatch")
	}

	// read
	for i := 0; i < len(tevs); i++ {
		v, err := log.Get(margaret.Seq(i))
		r.NoError(err, "failed to get event %d", i)

		ev, ok := v.(*testEvent)
		r.True(ok, "failed to cast event %d. got %T", i, v)
		r.Equal(*ev, tevs[i])
	}

	// cleanup
	if t.Failed() {
		t.Log("log was written to ", f.Name())
	} else {
		os.Remove(f.Name())
	}
}

// make sure that the sequence is picked up after opening an existing log
func TestWriteAndWriteAgain(t *testing.T) {
	//setup
	r := require.New(t)
	f, err := ioutil.TempFile("", t.Name())
	r.NoError(err)

	log, err := NewOffsetLog(f, lengthprefixed.New32(4096), mjson.New(&testEvent{}))
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
		r.Equal(margaret.Seq(i), seq, "sequence missmatch")
	}

	// close and open
	name := f.Name()
	f.Close()
	log = nil
	f, err = os.OpenFile(name, os.O_RDWR, 0600)
	r.NoError(err, "failed to re-open file")
	log, err = NewOffsetLog(f, lengthprefixed.New32(4096), mjson.New(&testEvent{}))
	r.NoError(err, "error during log creation")

	// fill again
	for i, ev := range tevs {
		seq, err := log.Append(ev)
		r.NoError(err, "failed to do 2nd append %d", i)
		r.Equal(margaret.Seq(len(tevs)+i), seq, "sequence missmatch %d", i)
	}

	// close
	f.Close()
	log = nil
	f, err = os.Open(name)
	r.NoError(err, "failed to re-open file")
	log, err = NewOffsetLog(f, lengthprefixed.New32(4096), mjson.New(&testEvent{}))
	r.NoError(err, "error during log creation")

	currSeq, err := log.Seq().Value()
	r.NoError(err, "failed to get current sequence")
	r.Equal(margaret.Seq(2*len(tevs)-1), currSeq.(margaret.Seq))

	// read by seq
	for i := 0; i < 2*len(tevs); i++ {
		v, err := log.Get(margaret.Seq(i))
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
		t.Log("log was written to ", f.Name())
	} else {
		os.Remove(f.Name())
	}
}
