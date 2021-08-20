// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

package offset2

import (
	"context"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"go.cryptoscope.co/luigi"
	mjson "go.cryptoscope.co/margaret/codec/json"
)

type testEvent struct {
	Foo string `json:",omitempty"`
	Bar int    `json:",omitempty"`
}

func TestReadWrite(t *testing.T) {
	//setup
	r := require.New(t)
	name, err := ioutil.TempDir("", t.Name())
	r.NoError(err)

	log, err := Open(name, mjson.New(&testEvent{}))
	r.NoError(err, "error during log creation")

	// cleanup
	defer func() {
		if t.Failed() {
			t.Logf("log data directory at %q was not deleted due to test failure", name)
		} else {
			os.RemoveAll(name)
		}
	}()

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
		r.Equal(int64(i), seq, "sequence missmatch")
	}

	// read
	for i := 0; i < len(tevs); i++ {
		v, err := log.Get(int64(i))
		r.NoError(err, "failed to get event %d", i)

		ev, ok := v.(*testEvent)
		r.True(ok, "failed to cast event %d. got %T", i, v)
		r.Equal(*ev, tevs[i])
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
		r.Equal(int64(i), seq, "sequence missmatch")
	}

	log, err = Open(name, mjson.New(&testEvent{}))
	r.NoError(err, "error during log creation")
	// fill again
	for i, ev := range tevs {
		seq, err := log.Append(ev)
		r.NoError(err, "failed to do 2nd append %d", i)
		r.Equal(int64(len(tevs)+i), seq, "sequence missmatch %d", i)
	}

	// close
	r.NoError(log.Close())

	_, err = log.Append(23)
	r.NotNil(err)

	log, err = Open(name, mjson.New(&testEvent{}))
	r.NoError(err, "error during log creation")

	currSeq := log.Seq()
	r.NoError(err, "failed to get current sequence")
	r.EqualValues(int64(2*len(tevs)-1), currSeq)

	// read by seq
	for i := 0; i < 2*len(tevs); i++ {
		v, err := log.Get(int64(i))
		r.NoError(err, "failed to get event %d", i)

		ev, ok := v.(*testEvent)
		r.True(ok, "failed to cast event %d. got %T", i, v)
		r.Equal(*ev, tevs[i%len(tevs)])
	}

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
		t.Log(v, seq)
		seq++
		// TODO: v has no sequence unless we put it in the values ourselvs..?
	}

	r.NoError(log.Close())
	// cleanup
	if t.Failed() {
		t.Log("log was written to ", name)
	} else {
		os.RemoveAll(name)
	}
}

// should be able to recover from journal in the future
func TestRecover(t *testing.T) {
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
		r.Equal(int64(i), seq, "sequence missmatch")
	}

	// close
	r.NoError(log.Close())

	// reopen and corrupt
	log, err = Open(name, mjson.New(&testEvent{}))
	r.NoError(err, "error during log open")

	// assuming journal was increased only
	seq, err := log.jrnl.bump()
	r.NoError(err)
	r.EqualValues(seq, len(tevs)) // +1-1

	r.NoError(log.Close())

	log, err = Open(name, mjson.New(&testEvent{}))
	r.NoError(err, "error while recover")
	r.NotNil(log)

	v := log.Seq()
	r.NoError(err, "error while recover")
	r.EqualValues(v, len(tevs)-1)
}
