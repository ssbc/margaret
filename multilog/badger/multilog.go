package badger // import "cryptoscope.co/go/margaret/multilog/badger"

import (
	"encoding/binary"
	"sync"

	"cryptoscope.co/go/luigi"
	"cryptoscope.co/go/margaret"
	"cryptoscope.co/go/margaret/multilog"
	"github.com/dgraph-io/badger"
	"github.com/pkg/errors"

	"cryptoscope.co/go/librarian"
)

func New(db *badger.DB, tipe interface{}) multilog.MultiLog {
	return &mlog{
		db:   db,
		tipe: tipe,

		sublogs: make(map[librarian.Addr]*sublog),
		curSeq:  -2,
	}
}

type mlog struct {
	l sync.Mutex

	db   *badger.DB
	tipe interface{}
	codec margaret.Codec

	prefixLen int

	sublogs map[librarian.Addr]*sublog
	curSeq  margaret.Seq
}

func (log *mlog) Get(addr librarian.Addr) (margaret.Log, error) {
	shortPrefix := []byte(addr)
	if len(shortPrefix) > log.prefixLen {
		return nil, errors.Errorf("supplied prefix longer than maximum prefix length %d", log.prefixLen)
	}

	zeroes := make([]byte, log.prefixLen-len(shortPrefix))
	prefix := append(zeroes, shortPrefix...)

	var seq margaret.Seq
	err := log.db.View(func(txn *badger.Txn) error {
		iter := txn.NewIterator(badger.IteratorOptions{Reverse: true})
		iter.Rewind()
		iter.Seek(prefix)
		if !iter.ValidForPrefix(prefix) {
			seq = -1
		} else {
			key := iter.Item().Key()
			seqBs := key[len(prefix):]
			if len(seqBs) != 8 {
				return errors.New("invalid key length (expected len(prefix)+8)")
			}
			seq = margaret.Seq(binary.BigEndian.Uint64(seqBs))
		}

		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "error in read transaction")
	}

	return &sublog{
		mlog:   log,
		prefix: prefix,
		seq:    luigi.NewObservable(seq),
	}, nil
}
