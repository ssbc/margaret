package multilog

import (
	"go.cryptoscope.co/librarian"
	"go.cryptoscope.co/margaret"
)

// MultiLog is a collection of logs, keyed by a librarian.Addr
// TODO maybe only call this log to avoid multilog.MultiLog?
type MultiLog interface {
	Get(librarian.Addr) (margaret.Log, error)
	Has(librarian.Addr) bool
	List() ([]librarian.Addr, error)
}
