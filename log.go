package margaret // import "cryptoscope.co/go/margaret"

import (
  "cryptoscope.co/go/luigi"
)

type Seq int64

const (
	SeqNoinit Seq = -1
)

type Log interface {
	Seq() luigi.Observable
	Get(Seq) (interface{}, error)
	Query(...QuerySpec) (luigi.Source, error)
	Append(interface{}) error
}

