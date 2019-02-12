package test

import (
	"testing"

	"go.cryptoscope.co/margaret/multilog/test"
)

func TestBadgerWithValues(t *testing.T) {
	t.Run("SubLog", test.RunSubLogTests)
	t.Run("MultiLog", test.RunMultiLogTests)
	t.Run("Sink", test.RunSinkTests)
}

// func ExampleRoaring() {
// 	r := roaring.New()

// 	r.Add(1)
// 	r.Add(2)
// 	r.Add(4)
// 	r.Add(5)
// 	r.Add(6)
// 	r.Add(8)

// 	r.AddRange(50, 500)

// 	r.AddRange(6000, 18000)

// 	for n := 5000; n > 0; n-- {
// 		r.Remove(rand.Uint32())
// 	}

// 	fmt.Println("init:", r.GetSerializedSizeInBytes())
// 	fmt.Println("compressed:", r.HasRunCompression())
// 	r.RunOptimize()
// 	fmt.Println("optimized:", r.GetSerializedSizeInBytes())
// 	fmt.Println("loaded:", r.GetSizeInBytes())
// 	fmt.Println("compressed:", r.HasRunCompression())
// 	goon.Dump(r.Stats())

// 	// Output:
// 	// init: 26
// 	// false
// 	// optimized: 19
// 	// loaded: 66
// 	// compressed true
// 	// len: 5
// }
