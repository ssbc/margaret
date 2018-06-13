package multilog

import "cryptoscope.co/go/margaret"

type Seq interface {
	Seq() margaret.Seq
	Name() string
}

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

type SeqStack interface {
	Seq

	Pop() Seq
}

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

type ValueSeq interface {
	Seq

	Value() interface{}
}

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
