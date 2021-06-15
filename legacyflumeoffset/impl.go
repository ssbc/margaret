// Package legacyflumeoffset implements the first iteration of an offset file.
// as implemented by https://github.com/flumedb/flumelog-offset
// Format
//
//data is stored in a append only log, where the byte index of the start of a record is the primary key (offset).
// offset-><data.length (UInt32BE)>
// <data ...>
// <data.length (UInt32BE)>
// <file_length (UInt32BE or Uint48BE or Uint53BE)>
// by writing the length of the data both before and after each record it becomes possible to scan forward and backward (like a doubly linked list)
// It's very handly to be able to scan backwards, as often you want to see the last N items, and so you don't need an index for this.
//
package legacyflumeoffset

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"

	"go.cryptoscope.co/luigi"
	"go.cryptoscope.co/margaret"
)

type Log struct {
	mu sync.Mutex

	codec margaret.Codec

	f *os.File
}

func Open(path string, codec margaret.Codec) (*Log, error) {
	os.MkdirAll(filepath.Dir(path), 0700)

	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0700)
	if err != nil {
		return nil, err
	}

	return &Log{
		f:     f,
		codec: codec,
	}, nil
}

// Seq returns an observable that holds the current sequence number
func (l *Log) Seq() luigi.Observable {
	panic("not implemented") // TODO: Implement
}

// readOffset reads and parses a frame.
func (log *Log) readOffset(ofst margaret.Seq) (interface{}, uint32, error) {
	r, entryLen, err := log.dataReader(ofst)
	if err != nil {
		return nil, 0, fmt.Errorf("error getting reader for ofst:%d: %w", ofst.Seq(), err)
	}

	dec := log.codec.NewDecoder(r)
	v, err := dec.Decode()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return v, 0, luigi.EOS{}
		}
		return nil, 0, fmt.Errorf("error decoding data for ofst:%d: %w", ofst.Seq(), err)
	}
	return v, entryLen, nil
}

func (log *Log) dataReader(ofst margaret.Seq) (io.Reader, uint32, error) {
	var sz uint32

	sizeRd := io.NewSectionReader(log.f, ofst.Seq(), 4)

	err := binary.Read(sizeRd, binary.BigEndian, &sz)
	if err != nil {
		return nil, 0, err
	}

	entryRd := io.NewSectionReader(log.f, ofst.Seq()+4, int64(sz))
	// next offset is 12 bytes after the size
	return entryRd, sz + 12, nil
}

// Get returns the entry with sequence number seq
func (l *Log) Get(ofst margaret.Seq) (interface{}, error) {
	v, _, err := l.readOffset(ofst)
	return v, err
}

// Query returns a stream that is constrained by the passed query specification
func (log *Log) Query(specs ...margaret.QuerySpec) (luigi.Source, error) {
	log.mu.Lock()
	defer log.mu.Unlock()

	qry := &lfoQuery{
		log:   log,
		codec: log.codec,

		nextOfst: margaret.SeqEmpty,
		lt:       margaret.SeqEmpty,

		limit: -1, //i.e. no limit
		close: make(chan struct{}),
	}

	for _, spec := range specs {
		err := spec(qry)
		if err != nil {
			return nil, err
		}
	}

	if qry.reverse && qry.live {
		return nil, errors.New("offset2: can't do reverse and live")
	}

	return qry, nil
}

// Append appends a new entry to the log
func (l *Log) Append(v interface{}) (margaret.Seq, error) {
	where, err := l.f.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}

	b, err := l.codec.Marshal(v)
	if err != nil {
		return nil, err
	}

	var sz = uint32(len(b))

	// offset/size of entry _before_ the entry
	err = binary.Write(l.f, binary.BigEndian, sz)
	if err != nil {
		return nil, err
	}

	// the actual entry
	_, err = l.f.Write(b)
	if err != nil {
		return nil, err
	}

	// offset/size of entry _after_ the entry
	err = binary.Write(l.f, binary.BigEndian, sz)
	if err != nil {
		return nil, err
	}

	// now the "final" file length marker

	if where > math.MaxUint32 {
		return nil, fmt.Errorf("legacyflumeoffset: size limit breaks uint32 - implement weird uint48 and 53 stuff")
	}

	// 3*4 because we have 3 uint32's
	next := uint32(where) + 3*4 + sz
	err = binary.Write(l.f, binary.BigEndian, next)
	if err != nil {
		return nil, err
	}

	return margaret.BaseSeq(where), nil
}
