package offset // import "go.cryptoscope.co/margaret/offset"

import (
	"context"
	"io"
	"os"
	"sync"

	"go.cryptoscope.co/luigi"
	"go.cryptoscope.co/margaret"
	"github.com/pkg/errors"
)

// DefaultFrameSize is the default frame size.
const DefaultFrameSize = 4096

type offsetLog struct {
	l sync.Mutex
	f *os.File

	seq     luigi.Observable
	codec   margaret.Codec
	framing margaret.Framing
}

// New returns a new offset log.
func New(f *os.File, framing margaret.Framing, cdc margaret.Codec) (margaret.Log, error) {
	log := &offsetLog{
		f:       f,
		framing: framing,
		codec:   cdc,
	}

	// get current sequence by end / blocksize
	end, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, errors.Wrap(err, "failed to seek to end of log-file")
	}
	// assumes -1 is SeqEmpty
	log.seq = luigi.NewObservable(margaret.Seq((end / framing.FrameSize()) - 1))

	return log, nil
}

func (log *offsetLog) Seq() luigi.Observable {
	return log.seq
}

func (log *offsetLog) Get(s margaret.Seq) (interface{}, error) {
	q, err := log.Query(margaret.Limit(1), margaret.Gte(s))
	if err != nil {
		return nil, errors.Wrap(err, "error constructing single-value query")
	}

	return q.Next(context.TODO())
}

func (log *offsetLog) Query(specs ...margaret.QuerySpec) (luigi.Source, error) {
	log.l.Lock()
	defer log.l.Unlock()

	qry := &offsetQuery{
		log:   log,
		codec: log.codec,

		nextSeq: margaret.SeqEmpty,
		lt:      margaret.SeqEmpty,

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

func (log *offsetLog) Append(v interface{}) (margaret.Seq, error) {
	data, err := log.codec.Marshal(v)
	if err != nil {
		return -1, errors.Wrap(err, "error marshaling value")
	}

	log.l.Lock()
	defer log.l.Unlock()

	_, err = log.f.Seek(0, io.SeekEnd)
	if err != nil {
		return -1, errors.Wrap(err, "error seeking to end of file")
	}

	frame, err := log.framing.EncodeFrame(data)
	if err != nil {
		return -1, err
	}

	_, err = log.f.Write(frame)
	if err != nil {
		return -1, errors.Wrap(err, "error writng frame")
	}

	currSeq, err := log.seq.Value()
	if err != nil {
		return -1, errors.Wrap(err, "error reading current sequence number")
	}
	nextSeq := currSeq.(margaret.Seq) + 1
	return nextSeq, log.seq.Set(nextSeq)
}

func (log *offsetLog) FileName() string {
	return log.f.Name()
}
