package multilog

import "go.cryptoscope.co/margaret"

// Seq is a named sequence number.
type Seq interface {
	margaret.Seq

	Name() string
}

// NewSeq returns a named sequence number.
func WithName(seq margaret.Seq, name string) Seq {
	return namedSeq{
		name: name,
		seq:  seq,
	}
}

type namedSeq struct {
	seq  margaret.Seq
	name string
}

func (seq namedSeq) Seq() int64 {
	return seq.seq.Seq()
}

func (seq namedSeq) Name() string {
	return seq.name
}

// ValueSeq binds a value to a sequence number.
type ValueSeq interface {
	Seq

	Value() interface{}
}

// WithValue returns a sequence number that has a value attached to it.
func WithValue(seq Seq, value interface{}) ValueSeq {
	return valSeq{
		seq:   seq,
		value: value,
	}
}

type valSeq struct {
	seq   Seq
	value interface{}
}

func (vs valSeq) Value() interface{} {
	return vs.value
}

func (vs valSeq) Seq() int64 {
	return vs.seq.Seq()
}

func (vs valSeq) Name() string {
	return vs.seq.Name()
}
