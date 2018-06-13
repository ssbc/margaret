package multilog

import "cryptoscope.co/go/margaret"

// Seq is a named sequence number.
type Seq interface {
	Seq() margaret.Seq
	Name() string
}

// NewSeq returns a named sequence number.
func NewSeq(seq margaret.Seq, name string) Seq {
	return namedSeq{
		name: name,
		seq:  seq,
	}
}

type namedSeq struct {
	seq  margaret.Seq
	name string
}

func (seq namedSeq) Seq() margaret.Seq {
	return seq.seq
}

func (seq namedSeq) Name() string {
	return seq.name
}

// SeqStack can handle a stack of sequence numbers, which is interesting
// when cascading logs because the entry has a different sequence number
// in each log.
type SeqStack interface {
	Seq

	Pop() Seq
}

// Push pushes a sequence number on the top of the stack.
func Push(lower Seq, newHead Seq) SeqStack {
	return &seqStack{
		seq:   newHead,
		lower: lower,
	}
}

type seqStack struct {
	seq   Seq
	lower Seq
}

func (seq *seqStack) Pop() Seq {
	return seq.lower
}

func (seq *seqStack) Seq() margaret.Seq {
	return seq.seq.Seq()
}

func (seq *seqStack) Name() string {
	return seq.seq.Name()
}

// ValueSeq binds a value to a sequence number.
type ValueSeq interface {
	Seq

	Value() interface{}
}

// WithValue returns a sequence number that has a value attached to it.
func WithValue(seq Seq, value interface{}) ValueSeq {
	for {
		_, ok := seq.(ValueSeq)
		if !ok {
			break
		}

		next, ok := seq.(SeqStack)
		if !ok {
			break
		}

		seq = next.Pop()
	}

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

func (vs valSeq) Pop() Seq {
	return vs.seq
}

func (vs valSeq) Seq() margaret.Seq {
	return vs.seq.Seq()
}

func (vs valSeq) Name() string {
	return vs.seq.Name()
}
