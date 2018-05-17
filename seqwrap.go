package margaret

type SeqWrapper interface {
	Seq() Seq
	Value() interface{}
}

type seqWrapper struct {
	seq Seq
	v   interface{}
}

func (sw *seqWrapper) Seq() Seq {
	return sw.seq
}

func (sw *seqWrapper) Value() interface{} {
	return sw.v
}

func WrapWithSeq(v interface{}, seq Seq) SeqWrapper {
	return &seqWrapper{
		seq: seq,
		v:   v,
	}
}
