package librarian // import "cryptoscope.co/go/librarian"

type Marshaler interface {
	Marshal() ([]byte, error)
}

type Unmarshaler interface {
	Unmarshal([]byte) error
}
