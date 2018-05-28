package margaret // import "cryptoscope.co/go/margaret"

type Query interface {
	Gt(Seq) error
	Gte(Seq) error
	Lt(Seq) error
	Lte(Seq) error
	Limit(int) error

	Live(bool) error
	SeqWrap(bool) error
}

type QuerySpec func(Query) error

func MergeQuerySpec(spec ...QuerySpec) QuerySpec {
	return func(qry Query) error {
		for _, f := range spec {
			err := f(qry)
			if err != nil {
				return err
			}
		}

		return nil
	}
}

func ErrorQuerySpec(err error) QuerySpec {
	return func(Query) error {
		return err
	}
}

func Gt(s Seq) QuerySpec {
	return func(q Query) error {
		return q.Gt(s)
	}
}

func Gte(s Seq) QuerySpec {
	return func(q Query) error {
		return q.Gte(s)
	}
}

func Lt(s Seq) QuerySpec {
	return func(q Query) error {
		return q.Lt(s)
	}
}

func Lte(s Seq) QuerySpec {
	return func(q Query) error {
		return q.Lte(s)
	}
}

func Limit(n int) QuerySpec {
	return func(q Query) error {
		return q.Limit(n)
	}
}

func Live(live bool) QuerySpec {
	return func(q Query) error {
		return q.Live(live)
	}
}

func SeqWrap(wrap bool) QuerySpec {
	return func(q Query) error {
		return q.SeqWrap(wrap)
	}
}
