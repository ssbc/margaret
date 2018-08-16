package test

import (
	"testing"
)

func TestPump(t *testing.T) {
	for k, v := range newLogFuncs {
		t.Run(k+"/pump", LogTestPump(v))
		t.Run(k+"/pumplive", LogTestPumpLive(v))
	}
}
