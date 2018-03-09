package margaret // import "cryptoscope.co/go/margaret"

type Query interface {
	Limit(int) error
	Live(bool) error
	Immediate(bool) error
}

type QuerySpec func(Query) error

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

func Immediate(imm bool) QuerySpec {
	return func(q Query) error {
		return q.Immediate(imm)
	}
}
