package test

import (
	"testing"

	mtest "cryptoscope.co/go/margaret/test"
)

var (
	NewSetterIndexFuncs    map[string]NewSetterIndexFunc
	NewSeqSetterIndexFuncs map[string]NewSeqSetterIndexFunc
)

func init() {
	NewSetterIndexFuncs    = map[string]NewSetterIndexFunc{}
	NewSeqSetterIndexFuncs = map[string]NewSeqSetterIndexFunc{}
}

func RegisterSetterIndex(name string, f NewSetterIndexFunc) {
	NewSetterIndexFuncs[name] = f
}

func RegisterSeqSetterIndex(name string, f NewSeqSetterIndexFunc) {
	NewSeqSetterIndexFuncs[name] = f
}

func RunSetterIndexTests(t *testing.T) {
	for name, newIndex := range NewSetterIndexFuncs {
		t.Run(name, TestSetterIndex(newIndex))
	}
}

func RunSeqSetterIndexTests(t *testing.T) {
	for name, newIndex := range NewSeqSetterIndexFuncs {
		t.Run(name, TestSeqSetterIndex(newIndex))
	}
}

func RunSinkIndexTests(t *testing.T) {
	for logname, newLog := range mtest.NewLogFuncs {
		for idxname, newSeqSetterIdx := range NewSeqSetterIndexFuncs {
			t.Run(logname + "/" + idxname, TestSinkIndex(newLog, newSeqSetterIdx))
		} 
	}
}
