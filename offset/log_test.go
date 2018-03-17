package offset // import "cryptoscope.co/go/margaret/offset"

import (
  "os"
  "strings"
  "testing"

  "cryptoscope.co/go/margaret"
  "cryptoscope.co/go/margaret/codec/json"
  "cryptoscope.co/go/margaret/test"
)

type testStruct struct{}

func JSONOffsetLog() func(*testing.T) {
  return func(t *testing.T) {
    f := func(name string, tipe interface{}) margaret.Log {
      name = strings.Replace(name, "/", "_", -1)
      f, err := os.Create(name)
      if err != nil {
        t.Fatal("error opening offset file", err)
      }

      return NewOffsetLog(f, json.NewCodec(tipe))
    }
    t.Run("JSON", test.LogTest(f))
  }
}

func TestOffsetLog(t *testing.T) {
  t.Run("Offset", JSONOffsetLog())
}
