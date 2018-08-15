package test

import (
  "testing"
)

func TestPump(t *testing.T) {
	for k, v := range newLogFuncs {
		t.Run(k, LogTestPump(v))
	}
}
