package margaret // import "go.cryptoscope.co/margaret"

import (
	"go.cryptoscope.co/luigi"
)

// Seq is the sequence number of an item in the log
// TODO currently this Seq and the one in multilog somewhat do the same but not really. Find a way to unify them.
type Seq int64

// Seq returns itself to adhere to the Seq interface in ./multilog.
func (s Seq) Seq() Seq {
	return s
}

// Name returns the name of the log. Since this is a basic. unwrapped sequence number, the name is always "root".
func (Seq) Name() string {
	return "root"
}

const (
	// SeqEmpty is the current sequence number of an empty log
	SeqEmpty Seq = -1
)

// Log stores entries sequentially, which can be queried individually using Get or as streams using Query.
type Log interface {
	// Seq returns an observable that holds the current sequence number
	Seq() luigi.Observable
	
	// Get returns the entry with sequence number seq
	Get(seq Seq) (interface{}, error)

	// Query returns a stream that is constrained by the passed query specification
	Query(...QuerySpec) (luigi.Source, error)

	// Append appends a new entry to the log
	Append(interface{}) (Seq, error)
}

type oob struct{}

// OOB is an out of bounds error
var OOB oob

func (oob) Error() string {
	return "out of bounds"
}

// IsOutOfBounds returns whether a particular error is an out-of-bounds error
func IsOutOfBounds(err error) bool {
	_, ok := err.(oob)
	return ok
}
