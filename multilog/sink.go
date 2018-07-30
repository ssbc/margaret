package multilog

import (
	"context"

	"github.com/pkg/errors"
	"go.cryptoscope.co/librarian"
	"go.cryptoscope.co/luigi"
	"go.cryptoscope.co/margaret"
)

// Func is a processing function that consumes a stream and sets values in the multilog.
type Func func(ctx context.Context, seq margaret.Seq, value interface{}, mlog MultiLog) error

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

// Has checks wether a log with that addr is in the mlog
func (slog *sinkLog) Has(addr librarian.Addr) bool {
	return slog.mlog.Has(addr)
}

// List returns all the
func (slog *sinkLog) List() []librarian.Addr {
	return slog.mlog.List()
}

// Pour calls the processing function to add a value to a sublog.
func (slog *sinkLog) Pour(ctx context.Context, v interface{}) error {
	seq := v.(margaret.SeqWrapper)
	err := slog.f(ctx, seq.Seq(), seq.Value(), slog.mlog)
	return errors.Wrap(err, "error in processing function")
}

// Close is a noop, but required for the Sink interface
func (*sinkLog) Close() error { return nil }

type roLog struct {
	margaret.Log
}

// Append always returns an error that indicates that this log is read only.
func (roLog) Append(v interface{}) (margaret.Seq, error) {
	return margaret.SeqEmpty, errors.New("can't append to read-only log")
}
