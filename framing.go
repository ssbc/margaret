package margaret // import "cryptoscope.co/go/margaret"

type Framing interface {
	DecodeFrame([]byte) ([]byte, error)
	EncodeFrame([]byte) ([]byte, error)
}
