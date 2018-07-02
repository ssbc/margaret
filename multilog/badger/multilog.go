package badger // import "go.cryptoscope.co/margaret/multilog/badger"

import (
	"encoding/binary"
	"sync"

	"github.com/dgraph-io/badger"
	"github.com/pkg/errors"
	"go.cryptoscope.co/luigi"
	"go.cryptoscope.co/margaret"
	"go.cryptoscope.co/margaret/multilog"

	"go.cryptoscope.co/librarian"
)

// New returns a new badger-backed multilog with given codec.
func New(db *badger.DB, tipe interface{}, codec margaret.Codec) multilog.MultiLog {
	return &mlog{
		db:   db,
		tipe: tipe,

		sublogs: make(map[librarian.Addr]*sublog),
		curSeq:  margaret.BaseSeq(-2),
		codec:   codec,
	}
}

type mlog struct {
	l sync.Mutex

	db    *badger.DB
	tipe  interface{}
	codec margaret.Codec

	sublogs map[librarian.Addr]*sublog
	curSeq  margaret.Seq
}

func (log *mlog) Get(addr librarian.Addr) (margaret.Log, error) {
	shortPrefix := []byte(addr)
	if len(shortPrefix) > 255 {
		return nil, errors.New("supplied address than maximum prefix length 255")
	}

	prefix := append([]byte{byte(len(shortPrefix))}, shortPrefix...)

	log.l.Lock()
	defer log.l.Unlock()

	slog := log.sublogs[librarian.Addr(prefix)]
	if slog != nil {
		return slog, nil
	}

	// find the current seq
	var seq margaret.Seq
	err := log.db.View(func(txn *badger.Txn) error {
		iter := txn.NewIterator(badger.IteratorOptions{Reverse: true})
		defer iter.Close()
		iter.Rewind()
		iter.Seek(prefix)
		if !iter.ValidForPrefix(prefix) {
			seq = margaret.SeqEmpty
		} else {
			key := iter.Item().Key()
			seqBs := key[len(prefix):]
			if len(seqBs) != 8 {
				return errors.New("invalid key length (expected len(prefix)+8)")
			}
			seq = margaret.BaseSeq(binary.BigEndian.Uint64(seqBs))
		}

		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "error in read transaction")
	}

	slog = &sublog{
		mlog:   log,
		prefix: prefix,
		seq:    luigi.NewObservable(seq),
	}

	log.sublogs[librarian.Addr(prefix)] = slog
	return slog, nil
}
