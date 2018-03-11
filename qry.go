package margaret // import "cryptoscope.co/go/margaret"

type Query interface {
	Gt(Seq) error
	Gte(Seq) error
	Lt(Seq) error
	Lte(Seq) error
	Limit(int) error

	Live(bool) error
}

type QuerySpec func(Query) error

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
