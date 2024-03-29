// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

package indexes

import (
	"context"
	"fmt"

	"github.com/ssbc/go-luigi"
	"github.com/ssbc/margaret"
)

type StreamProcFunc func(context.Context, int64, interface{}, SetterIndex) error

func NewSinkIndex(f StreamProcFunc, idx SeqSetterIndex) SinkIndex {
	return &sinkIndex{
		idx: idx,
		f:   f,
	}
}

type sinkIndex struct {
	idx SeqSetterIndex
	f   StreamProcFunc
}

func (r *sinkIndex) QuerySpec() margaret.QuerySpec {
	seq, err := r.idx.GetSeq()
	if err != nil {
		// wrap error in erroring queryspec
		return margaret.ErrorQuerySpec(err)
	}

	return margaret.MergeQuerySpec(margaret.Gt(seq), margaret.SeqWrap(true))
}

func (idx *sinkIndex) Pour(ctx context.Context, v interface{}) error {
	switch tv := v.(type) {
	case margaret.SeqWrapper:
		err := idx.f(ctx, tv.Seq(), tv.Value(), idx.idx)
		if err != nil {
			return fmt.Errorf("error calling setter func: %w", err)
		}
		err = idx.idx.SetSeq(tv.Seq())
		if err != nil {
			return fmt.Errorf("error setting sequence number: %w", err)
		}
		return nil
	case error:
		if margaret.IsErrNulled(tv) {
			return nil
		}
		return tv

	default:
		return fmt.Errorf("expecting seqwrapped value (%T)", v)
	}

}

func (idx *sinkIndex) Close() error {
	return idx.idx.Close()
}

func (idx *sinkIndex) Get(ctx context.Context, a Addr) (luigi.Observable, error) {
	return idx.idx.Get(ctx, a)
}
