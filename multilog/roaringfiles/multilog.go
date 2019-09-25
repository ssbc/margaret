package roaringfiles

import (
	"fmt"
	stdlog "log"
	"os"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/pkg/errors"
	"go.cryptoscope.co/librarian"
	"go.cryptoscope.co/luigi"

	"go.cryptoscope.co/margaret"
	"go.cryptoscope.co/margaret/internal/persist"
	"go.cryptoscope.co/margaret/internal/persist/fs"
	"go.cryptoscope.co/margaret/internal/persist/mkv"
	"go.cryptoscope.co/margaret/internal/persist/sqlite"
	"go.cryptoscope.co/margaret/multilog"
)

// New returns a new multilog that is only good to store sequences
// It uses files to store roaring bitmaps directly.
// for this it turns the librarian.Addrs into a hex string.

func NewFS(base string) multilog.MultiLog {
	return newAbstract(fs.New(base))
}

func NewSQLite(base string) (multilog.MultiLog, error) {
	s, err := sqlite.New(base)
	if err != nil {
		return nil, err
	}
	return newAbstract(s), nil
}

func NewMKV(base string) (multilog.MultiLog, error) {
	s, err := mkv.New(base)
	if err != nil {
		return nil, err
	}
	return newAbstract(s), nil
}

func newAbstract(store persist.Saver) multilog.MultiLog {
	return &mlog{
		store:   store,
		sublogs: make(map[librarian.Addr]*sublog),
		curSeq:  margaret.BaseSeq(-2),
	}
}

type mlog struct {
	store persist.Saver

	curSeq margaret.Seq

	l       sync.Mutex
	sublogs map[librarian.Addr]*sublog
}

func (log *mlog) Get(addr librarian.Addr) (margaret.Log, error) {
	log.l.Lock()
	defer log.l.Unlock()

	key := []byte(addr)

	slog := log.sublogs[addr]
	if slog != nil {
		return slog, nil
	}

	var seq margaret.Seq
	r, err := log.loadBitmap(key)
	if errors.Cause(err) == persist.ErrNotFound {
		seq = margaret.SeqEmpty
		r = roaring.New()
	} else if err != nil {
		return nil, err
	} else {
		seq = margaret.BaseSeq(r.GetCardinality() - 1)
	}

	slog = &sublog{
		mlog:   log,
		key:    key,
		seq:    luigi.NewObservable(seq),
		bmap:   r,
		notify: make(chan uint64),
	}
	if dbdr := os.Getenv("DEBOUNCE"); dbdr != "" {
		durr, err := time.ParseDuration(dbdr)
		if err != nil {
			durr = 15 * time.Second
		}
		fmt.Println("warning: experimental debouncing", durr)
		slog.debounce = true
		// TODO: move these updates to a single mlog update thing, if you really want this
		go debounce(durr, slog.notify, slog.update)
	}
	log.sublogs[librarian.Addr(addr)] = slog
	return slog, nil
}

func debounce(interval time.Duration, notify chan uint64, cb func() error) {
	timer := time.NewTimer(interval)
	for {
		select {
		case _ = <-notify:
			timer.Reset(interval)
		case <-timer.C:
			if err := cb(); err != nil {
				panic(err)
			}
		}
	}
}

func (log *mlog) loadBitmap(key []byte) (*roaring.Bitmap, error) {
	var r *roaring.Bitmap

	data, err := log.store.Get(key)
	if err != nil {
		return nil, errors.Wrapf(err, "roaringfiles: invalid stored bitfield %x", key)
	}

	r = roaring.New()
	err = r.UnmarshalBinary(data)
	if err != nil {
		return nil, errors.Wrapf(err, "roaringfiles: unpack of %x failed", key)
	}

	if _, err := log.tryCompress(persist.Key(key), r); err != nil {
		return nil, errors.Wrapf(err, "roaringfiles: loadCompress failed for %x", key)
	}

	return r, nil
}

func (log *mlog) tryCompress(key persist.Key, r *roaring.Bitmap) (bool, error) {
	n := r.GetSizeInBytes()
	if n > 4*1024 {
		currSize := r.GetSerializedSizeInBytes()
		r.RunOptimize()
		newSize := r.GetSerializedSizeInBytes()

		if currSize > newSize {
			compressed, err := r.MarshalBinary()
			if err != nil {
				return false, errors.Wrap(err, "roaringfiles: compress marshal failed")
			}
			err = log.store.Put(key, compressed)
			if err != nil {
				return false, errors.Wrap(err, "roaringfiles: write compressed failed")
			}
			stdlog.Printf("roaringfiles/compress(%x): reduced to %d (%d entries)\n", key, newSize, n)
			return true, nil
		}
	}
	return false, nil
}

// List returns a list of all stored sublogs
func (log *mlog) List() ([]librarian.Addr, error) {
	log.l.Lock()
	defer log.l.Unlock()

	var list []librarian.Addr

	keys, err := log.store.List()
	if err != nil {
		return nil, errors.Wrap(err, "roaringfiles: store iteration failed")
	}
	for _, bk := range keys {
		bmap, err := log.loadBitmap(bk)
		if err != nil {
			return nil, errors.Wrapf(err, "roaringfiles: broken bitmap file (%x)", bk)
		}

		if bmap.GetCardinality() == 0 {
			continue
		}
		list = append(list, librarian.Addr(bk))
	}
	return list, errors.Wrap(err, "roaringfiles: error in List() iteration")
}

func (log *mlog) Close() error {
	return log.store.Close()
}
