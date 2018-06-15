package remote

import (
	"context"

	"go.cryptoscope.co/luigi"
	"github.com/pkg/errors"

	"go.cryptoscope.co/librarian"
)

func NewSetter(sink luigi.Sink) librarian.Setter {
	return &setter{sink}
}

type setter struct {
	sink luigi.Sink
}

func (s *setter) Set(ctx context.Context, addr librarian.Addr, v interface{}) error {
	return errors.Wrap(s.sink.Pour(ctx, Message{
		Type:  "set",
		Addr:  addr,
		Value: v,
	}), "error pouring set message")
}

func (s *setter) Delete(ctx context.Context, addr librarian.Addr) error {
	return errors.Wrap(s.sink.Pour(ctx, Message{
		Type: "delete",
		Addr: addr,
	}), "error pouring delete message")
}
