package multilog

import (
	"context"

	"go.cryptoscope.co/luigi"
	"go.cryptoscope.co/margaret"
	"github.com/pkg/errors"
)

// NameLog attaches a name to a log, such that the seq-value pairs returned from queried sources implement ValueSeq.
func NameLog(log margaret.Log, name string) margaret.Log {
	return namedLog{log, name}
}

// namedLog wraps a log and attaches names to
type namedLog struct {
	margaret.Log
	name string
}

func (log namedLog) Query(specs ...margaret.QuerySpec) (luigi.Source, error) {
	src, err := log.Log.Query(specs...)
	if err != nil {
		return nil, errors.Wrap(err, "error returned by underlying log")
	}

	return namedQuery{src, log.name}, nil
}

type namedQuery struct {
	luigi.Source
	name string
}

func (qry namedQuery) Next(ctx context.Context) (interface{}, error) {
	v, err := qry.Source.Next(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "error returned by underlying Source")
	}

	sw, ok := v.(margaret.SeqWrapper)
	if !ok {
		return nil, errors.Errorf("expected a ValueSeq, got a %T", v)
	}

	return namedValueSeq{sw, qry.name}, nil
}

type namedValueSeq struct {
	margaret.SeqWrapper
	name string
}

func (seq namedValueSeq) Name() string {
	return seq.name
}
