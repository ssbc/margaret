package mkv

import (
	"io"

	"go.cryptoscope.co/margaret/internal/persist"
)

func (s ModernSaver) Put(key persist.Key, data []byte) error {
	return s.db.Set(key, data)
}

func (s ModernSaver) Get(key persist.Key) ([]byte, error) {
	data, err := s.db.Get(nil, key)
	if data == nil {
		return nil, persist.ErrNotFound
	}
	return data, err
}

func (s ModernSaver) List() ([]persist.Key, error) {
	var keys []persist.Key
	iter, err := s.db.SeekFirst()
	if err != nil {
		if err == io.EOF {
			return keys, nil
		}
		return nil, err
	}
	for {
		k, _, err := iter.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		keys = append(keys, k)
	}
	return keys, nil
}
