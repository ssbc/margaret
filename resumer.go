package librarian

import (
	"context"

	"cryptoscope.co/go/luigi"
	"cryptoscope.co/go/margaret"
	"github.com/pkg/errors"
)

type SeqSetterIndex interface {
	SetterIndex

	SetSeq(margaret.Seq) error
	GetSeq() (margaret.Seq, error)
}

func NewSinkIndex(f func(ctx context.Context, v interface{}, idx SetterIndex) error, idx SeqSetterIndex) SinkIndex {
	return &sinkIndex{
		idx: idx,
		f:   f,
	}
}

type sinkIndex struct {
	idx SeqSetterIndex
	f   func(ctx context.Context, v interface{}, idxset SetterIndex) error
}

func (r *sinkIndex) QuerySpec() (margaret.QuerySpec, error) {
	seq, err := r.idx.GetSeq()
	if err != nil {
		return nil, errors.Wrap(err, "error getting sequence number")
	}

	return margaret.Gt(seq), nil
}

func (idx *sinkIndex) Pour(ctx context.Context, v interface{}) error {
	err := idx.f(ctx, v, idx.idx)
	if err != nil {
		return errors.Wrap(err, "error calling setter func")
	}

	seqer, ok := v.(interface{ Seq() margaret.Seq })
	if ok {
		errors.Wrap(idx.idx.SetSeq(seqer.Seq()), "error setting sequence number")
	}

	return nil
}

func (idx *sinkIndex) Close() error {
	// TODO implement index closing
	return nil
}

func (idx *sinkIndex) Get(ctx context.Context, a Addr) (luigi.Observable, error) {
	return idx.idx.Get(ctx, a)
}
