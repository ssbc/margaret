package sqlite

import (
	"database/sql"
	"encoding/hex"
	"fmt"

	"github.com/pkg/errors"
	"go.cryptoscope.co/margaret/internal/persist"
)

func (s SqliteSaver) Put(key persist.Key, data []byte) error {
	hexKey := hex.EncodeToString(key)
	fmt.Printf("DEUBUG/put: %s %q\n", hexKey, data)
	_, err := s.db.Exec(`delete from persisted_roaring where key = ?; insert into persisted_roaring (key,data) VALUES(?,?)`, hexKey, hexKey, data)
	if err != nil {
		return errors.Wrap(err, "sqlite/put: failed run delete/insert value")
	}
	return nil
}

func (s SqliteSaver) Get(key persist.Key) ([]byte, error) {

	var data []byte
	hexKey := hex.EncodeToString(key)
	err := s.db.QueryRow(`SELECT data from persisted_roaring where key = ?`, hexKey).Scan(&data)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, persist.ErrNotFound
		}
		return nil, errors.Wrapf(err, "persist/sqlite/get(%d): failed to execute query")
	}
	return data, nil
}

func (s SqliteSaver) List() ([]persist.Key, error) {
	var keys []persist.Key
	rows, err := s.db.Query(`SELECT key from persisted_roaring`)
	if err != nil {
		return nil, errors.Wrap(err, "persist/sqlite/list: failed to execute rows query")
	}
	defer rows.Close()

	for rows.Next() {
		var k string
		err := rows.Scan(&k)
		if err != nil {
			return nil, errors.Wrap(err, "persist/sqlite/list: failed to scan row result")
		}
		fmt.Printf("DEUBUG/list: %q\n", k)
		bk, err := hex.DecodeString(k)
		if err != nil {
			return nil, errors.Wrapf(err, "persist/sqlite/list: invalid key: %q", k)
		}
		keys = append(keys, bk)
	}

	return keys, rows.Err()
}
