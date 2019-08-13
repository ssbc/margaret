package sqlite

import (
	"bytes"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	"github.com/Masterminds/squirrel"

	_ "github.com/mattn/go-sqlite3"
	"github.com/pkg/errors"
	"go.cryptoscope.co/luigi"

	"go.cryptoscope.co/margaret"
)

func Open(path string, c margaret.Codec) (*sqliteLog, error) {
	s, err := os.Stat(path)
	if os.IsNotExist(err) {
		if filepath.Dir(path) == "" {
			path = "."
		}
		err = os.MkdirAll(path, 0700)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create path location")
		}
		s, err = os.Stat(path)
		if err != nil {
			return nil, errors.Wrap(err, "failed to stat created path location")
		}
	} else if err != nil {
		return nil, errors.Wrap(err, "failed to stat path location")
	}
	if s.IsDir() {
		path = filepath.Join(path, "log.db")
	}

	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to open sqlite file: %s", path)
	}
	var version int
	err = db.QueryRow(`PRAGMA user_version`).Scan(&version)
	if err == sql.ErrNoRows || version == 0 { // new file or old schema

		if _, err := db.Exec(schemaVersion1); err != nil {
			return nil, errors.Wrap(err, "margaret/sqlite: failed to init schema v1")
		}

	} else if err != nil {
		return nil, errors.Wrapf(err, "margaret/sqlite: schema version lookup failed %s", path)
	}

	return &sqliteLog{
		db: db,
		c:  c,
	}, err
}

const schemaVersion1 = `
CREATE TABLE margaret_log (
	id INTEGER PRIMARY KEY,
	data blob
);
PRAGMA user_version = 1;
`

type sqliteLog struct {
	db *sql.DB

	c margaret.Codec
}

func (sl sqliteLog) DB() *sql.DB { return sl.db }

func (sl sqliteLog) Close() error {
	return sl.db.Close()
}

func (sl sqliteLog) Seq() luigi.Observable {
	var cnt uint
	err := sl.db.QueryRow(`SELECT count(*) from margaret_log;`).Scan(&cnt)
	if err != nil {
		return luigi.NewObservable(err)
	}
	return luigi.NewObservable(margaret.BaseSeq(cnt - 1))
}

func (sl sqliteLog) Get(s margaret.Seq) (interface{}, error) {
	var data []byte
	err := sl.db.QueryRow(`SELECT data from margaret_log where id = ?`, s.Seq()+1).Scan(&data)
	if err != nil {
		return nil, errors.Wrapf(err, "sqlite/get(%d): failed to execute query", s.Seq())
	}
	v, err := sl.c.NewDecoder(bytes.NewReader(data)).Decode()
	return v, errors.Wrapf(err, "sqlite/get(%d): failed to decode value", s.Seq())
}

func (sl sqliteLog) Append(val interface{}) (margaret.Seq, error) {
	var buf bytes.Buffer
	err := sl.c.NewEncoder(&buf).Encode(val)
	if err != nil {
		return nil, errors.Wrap(err, "sqlite/append: failed to encode value")
	}

	res, err := sl.db.Exec(`insert into margaret_log (data) VALUES(?)`, buf.Bytes())
	if err != nil {
		return nil, errors.Wrap(err, "sqlite/append: failed insert new value")
	}

	newID, err := res.LastInsertId()
	if err != nil {
		return nil, errors.Wrap(err, "sqlite/append: failed to establish ID")
	}
	return margaret.BaseSeq(newID - 1), nil
}

func (sl sqliteLog) Query(specs ...margaret.QuerySpec) (luigi.Source, error) {
	// rows, err := sl.db.Query(`SELECT data from margaret_log`)
	// if err != nil {
	// 	return nil, errors.Wrap(err, "sqlite/query: failed to construct query object")
	// }

	qry := &sqliteQry{
		builder: squirrel.Select("data").From("margaret_log"),
		db:      sl.db,
		rows:    nil,
		c:       sl.c,
	}

	for _, s := range specs {
		err := s(qry)
		if err != nil {
			return nil, err
		}
	}

	return qry, nil
}

func (sl sqliteLog) Null(s margaret.Seq) error {
	rows, err := sl.db.Exec(`UPDATE margaret_log SET data = null where id = ?`, s.Seq()+1)
	if err != nil {
		return errors.Wrap(err, "sqlite/null: failed to execute update query")
	}
	affected, err := rows.RowsAffected()
	if err != nil {
		return errors.Wrap(err, "sqlite/null: rows affected failed")
	}
	if affected != 1 {
		return errors.Errorf("sqlite/null: not one row affected but %d", affected)
	}
	fmt.Println("entry nulled:", s.Seq()+1)
	return nil
}
