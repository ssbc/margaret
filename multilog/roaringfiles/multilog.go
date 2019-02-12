package roaringfiles

import (
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/pkg/errors"
	"go.cryptoscope.co/librarian"
	"go.cryptoscope.co/luigi"

	"go.cryptoscope.co/margaret"
	"go.cryptoscope.co/margaret/multilog"
)

// New returns a new multilog that is only good to store sequences
// It uses files to store roaring bitmaps directly.
// for this it turns the librarian.Addrs into a hex string.
func New(base string) multilog.MultiLog {
	return &mlog{
		base:    base,
		sublogs: make(map[librarian.Addr]*sublog),
		curSeq:  margaret.BaseSeq(-2),
	}
}

type mlog struct {
	base string

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
	if os.IsNotExist(errors.Cause(err)) {
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

func debounce(interval time.Duration, notify chan uint64, cb func(uint64)) {
	timer := time.NewTimer(interval)
	for {
		select {
		case _ = <-notify:
			timer.Reset(interval)
		case <-timer.C:
			cb(0)
		}
	}
}

func (log *mlog) loadBitmap(key []byte) (*roaring.Bitmap, error) {
	var r *roaring.Bitmap

	data, err := ioutil.ReadFile(log.fnameForKey(key))
	if err != nil {
		return nil, errors.Wrap(err, "roaringfiles/get: error in read transaction")
	}

	r = roaring.New()
	err = r.UnmarshalBinary(data)
	if err != nil {
		return nil, errors.Wrap(err, "roaringfiles: invalid stored bitfield")
	}

	return r, nil
}

func (log *mlog) fnameForKey(k []byte) string {
	var fname string
	hexKey := hex.EncodeToString(k)
	if len(hexKey) > 10 {
		fname = filepath.Join(log.base, hexKey[:5], hexKey[5:])
		os.MkdirAll(filepath.Dir(fname), 0700)
	} else {
		fname = filepath.Join(log.base, hexKey)
	}
	return fname
}

// List returns a list of all stored sublogs
func (log *mlog) List() ([]librarian.Addr, error) {
	var list []librarian.Addr

	err := filepath.Walk(log.base, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}

		name := strings.TrimPrefix(path, log.base+"/")
		if name[5] == '/' {
			var b = []byte(name)
			b = append(b[:5], b[6:]...)
			name = string(b)
		}
		bk, err := hex.DecodeString(name)
		if err != nil {
			return errors.Wrap(err, "roaringfiles: invalid path")
		}

		bmap, err := log.loadBitmap(bk)
		if err != nil {
			return errors.Wrapf(err, "roaringfiles: broken bitmap file (%q)", path)
		}

		if bmap.GetCardinality() == 0 {
			return nil
		}

		list = append(list, librarian.Addr(bk))
		return nil
	})

	return list, errors.Wrap(err, "roaringfiles: error in List() iteration")
}

func (log *mlog) Close() error {
	for key, slog := range log.sublogs {
		r := slog.bmap
		if r.GetCardinality() > 0 && !r.HasRunCompression() {
			old := r.GetSerializedSizeInBytes()
			r.RunOptimize()

			compressed, err := r.MarshalBinary()
			if err != nil {
				return errors.Wrap(err, "roaringfiles: marshal failed")
			}

			err = ioutil.WriteFile(log.fnameForKey([]byte(key)), compressed, 0700)
			if err != nil {
				return errors.Wrap(err, "roaringfiles: write update failed")
			}
			if old > uint64(len(compressed)) {
				fmt.Printf("roadingfiles: compressed roaring file %x from %d to %d\n", key, old, len(compressed))
			}
		}
	}
	return nil
}
