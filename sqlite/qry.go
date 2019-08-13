package sqlite

import (
	"bytes"
	"context"
	"database/sql"

	"github.com/Masterminds/squirrel"

	"github.com/pkg/errors"
	"go.cryptoscope.co/luigi"

	"go.cryptoscope.co/margaret"
)

func newQuery() {

}

type sqliteQry struct {
	db   *sql.DB
	rows *sql.Rows

	builder squirrel.SelectBuilder

	c margaret.Codec

	seqWrap bool
}

func (sq *sqliteQry) Next(ctx context.Context) (interface{}, error) {
	if sq.rows == nil {
		var err error
		sq.rows, err = sq.builder.RunWith(sq.db).Query()
		if err != nil {
			return nil, errors.Wrap(err, "sqlite/query: failed to init rows")
		}
	}

	if !sq.rows.Next() {
		return nil, luigi.EOS{}
	}

	var (
		id   int64
		data []byte
		err  error
	)
	if sq.seqWrap {
		err = sq.rows.Scan(&data, &id)
	} else {
		err = sq.rows.Scan(&data)
	}
	if err != nil {
		return nil, errors.Wrap(err, "sqlite/query: failed to scan data")
	}
	if len(data) == 0 {
		return nil, margaret.ErrNulled
	}

	v, err := sq.c.NewDecoder(bytes.NewReader(data)).Decode()
	if err != nil {
		return nil, errors.Wrap(err, "sqlite/query: next failed to decode data")
	}

	if sq.seqWrap {
		v = margaret.WrapWithSeq(v, margaret.BaseSeq(id-1))
	}
	return v, nil
}

func (qry *sqliteQry) Gt(s margaret.Seq) error {
	qry.builder = qry.builder.Where(squirrel.Gt{"id": s.Seq() + 1})
	return nil
}

func (qry *sqliteQry) Gte(s margaret.Seq) error {
	qry.builder = qry.builder.Where(squirrel.GtOrEq{"id": s.Seq() + 1})
	return nil
}

func (qry *sqliteQry) Lt(s margaret.Seq) error {
	qry.builder = qry.builder.Where(squirrel.Lt{"id": s.Seq() + 1})
	return nil
}

func (qry *sqliteQry) Lte(s margaret.Seq) error {
	qry.builder = qry.builder.Where(squirrel.LtOrEq{"id": s.Seq() + 1})
	return nil
}

func (qry *sqliteQry) Limit(n int) error {
	qry.builder = qry.builder.Limit(uint64(n))
	return nil
}

func (qry *sqliteQry) Live(live bool) error {
	if live {
		return margaret.ErrUnsupported("sqlite queries don't support live results")
	}
	return nil
}

func (qry *sqliteQry) SeqWrap(wrap bool) error {
	if wrap && qry.seqWrap == false { // only wrap once
		qry.builder = qry.builder.Columns("id")
		qry.seqWrap = wrap
	}
	return nil
}

func (qry *sqliteQry) Reverse(yes bool) error {
	if yes {
		qry.builder = qry.builder.OrderBy("id desc")
	}
	return nil
}
