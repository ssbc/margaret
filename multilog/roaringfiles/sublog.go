// SPDX-License-Identifier: MIT

package roaringfiles

import (
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/pkg/errors"
	"go.cryptoscope.co/luigi"
	"go.cryptoscope.co/margaret"
	"go.cryptoscope.co/margaret/internal/persist"
)

type sublog struct {
	mlog *MultiLog

	sync.Mutex
	key  persist.Key
	seq  luigi.Observable
	bmap *roaring.Bitmap

	lastSave uint64
}

func (log *sublog) Seq() luigi.Observable {
	return log.seq
}

func (log *sublog) Get(seq margaret.Seq) (interface{}, error) {
	log.Mutex.Lock()
	defer log.Mutex.Unlock()
	if seq.Seq() < 0 {
		return nil, luigi.EOS{}
	}

	v, err := log.bmap.Select(uint32(seq.Seq()))
	if err != nil {
		return nil, luigi.EOS{}
		// return nil, errors.Wrapf(luigi.EOS{}, "roaringfiles: bitmap access err (%s)", err)
	}
	return margaret.BaseSeq(v), err
}

func (log *sublog) Query(specs ...margaret.QuerySpec) (luigi.Source, error) {
	log.Mutex.Lock()
	defer log.Mutex.Unlock()
	qry := &query{
		log:  log,
		bmap: log.bmap.Clone(),

		lt:      margaret.SeqEmpty,
		nextSeq: margaret.SeqEmpty,

		limit: -1, //i.e. no limit
	}

	for _, spec := range specs {
		err := spec(qry)
		if err != nil {
			return nil, err
		}
	}

	return qry, nil
}

func (log *sublog) Append(v interface{}) (margaret.Seq, error) {
	log.Mutex.Lock()
	defer log.Mutex.Unlock()
	val, ok := v.(margaret.BaseSeq)
	if !ok {
		switch tv := v.(type) {
		case int:
			val = margaret.BaseSeq(tv)
		case int64:
			val = margaret.BaseSeq(tv)
		case uint32:
			val = margaret.BaseSeq(tv)
		default:
			return margaret.BaseSeq(-2), errors.Errorf("roaringfiles: not a sequence (%T)", v)
		}
	}
	if val.Seq() < 0 {
		return nil, errors.Errorf("roaringfiles can only store positive numbers")
	}

	log.bmap.Add(uint32(val.Seq()))
	count := log.bmap.GetCardinality() - 1

	cnt := margaret.BaseSeq(count)
	if err := log.update(); err != nil {
		return nil, err
	}

	log.seq.Set(cnt)
	return cnt, nil
}

func (log *sublog) update() error {
	// TODO: make store a bitmapStore, then we can also skip uncesessary unmarshals
	data, err := log.bmap.MarshalBinary()
	if err != nil {
		return errors.Wrap(err, "roaringfiles: failed to encode bitmap")
	}

	err = log.mlog.store.Put(log.key, data)
	if err != nil {
		return errors.Wrap(err, "roaringfiles: file write failed")
	}

	err = log.seq.Set(margaret.BaseSeq(log.bmap.GetCardinality() - 1))
	if err != nil {
		err = errors.Wrap(err, "roaringfiles: failed to update sequence")
		return err
	}

	return nil
}
