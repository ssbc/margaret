package multilog

import (
	"context"

	"cryptoscope.co/go/librarian"
	"cryptoscope.co/go/luigi"
	"cryptoscope.co/go/margaret"
	"github.com/pkg/errors"
)

type Func func(ctx context.Context, seq Seq, value interface{}, mlog MultiLog) error

// Sink is both a multilog and a luigi sink. Pouring values into it will append values to the multilog, usually by calling a user-defined processing function.
type Sink interface {
	MultiLog
	luigi.Sink
}

// NewSink makes a new Sink by wrapping a MultiLog and a processing function of type Func.
func NewSink(mlog MultiLog, f Func) Sink {
	return &sinkLog{
		mlog: mlog,
		f:    f,
	}
}

type sinkLog struct {
	mlog MultiLog
	f    Func
}

// Get gets the sublog with the given address.
func (slog *sinkLog) Get(addr librarian.Addr) (margaret.Log, error) {
	log, err := slog.mlog.Get(addr)
	if err != nil {
		return nil, errors.Wrap(err, "error getting log from multilog")
	}

	return roLog{log}, nil
}

// Pour calls the processing function to add a value to a sublog.
func (slog *sinkLog) Pour(ctx context.Context, v interface{}) error {
	seq := v.(ValueSeq)
	err := slog.f(ctx, seq, seq.Value, slog.mlog)
	return errors.Wrap(err, "error in processing function")
}

// Close is a noop, but required for the Sink interface
func (*sinkLog) Close() error { return nil }

type roLog struct {
	margaret.Log
}

// Append always returns an error that indicates that this log is read only.
func (roLog) Append(v interface{}) (margaret.Seq, error) {
	return -1, errors.New("can't append to read-only log")
}
