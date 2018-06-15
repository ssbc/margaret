package remote

import (
	"context"

	"go.cryptoscope.co/luigi"
	"github.com/pkg/errors"

	"go.cryptoscope.co/librarian"
)

var _ luigi.Sink = &indexSink{}

func NewSink(idx librarian.SetterIndex) luigi.Sink {
	return &indexSink{idx}
}

type indexSink struct {
	idx librarian.SetterIndex
}

func (sink *indexSink) Pour(ctx context.Context, v interface{}) error {
	msg := v.(Message)

	/*
		for _, set := range msg.Sets {
			err := sink.idx.Set(ctx, set.Addr, set.Value)
			if err != nil {
				return errors.Wrap(err, "errors setting value")
			}
		}

		for _, del := range msg.Deletes {
			err := sink.idx.Delete(ctx, del.Addr)
			if err != nil {
				return errors.Wrap(err, "errors deleting value")
			}
		}
	*/

	switch msg.Type {
	case "set":
		err := sink.idx.Set(ctx, msg.Addr, msg.Value)
		if err != nil {
			return errors.Wrap(err, "errors setting value")
		}
	case "delete":
		err := sink.idx.Delete(ctx, msg.Addr)
		if err != nil {
			return errors.Wrap(err, "errors deleting value")
		}
	default:
		return errors.Errorf("unknown message type %q", msg.Type)
	}

	return nil
}

func (sink *indexSink) Close() error { return nil } // noop
