// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

package seqobsv_test

import (
	"fmt"
	"testing"

	"go.cryptoscope.co/margaret/internal/seqobsv"
)

func ExampleInc() {
	sobs := seqobsv.New(0)
	fmt.Println(sobs.Value())

	newV := sobs.Inc()
	fmt.Println(newV)

	fmt.Println(sobs.Inc())

	// Output:
	// 0
	// 1
	// 2
}

func TestWaitSimple(t *testing.T) {

	sobs := seqobsv.New(0)

	if sobs.Value() != 0 {
		t.Fatal("start should be 0")
	}
	ch := sobs.WaitFor(4)

	go func() {
		for i := 0; i < 5; i++ {
			sobs.Inc()
		}
	}()

	<-ch

	if sobs.Value() < 4 {
		t.Fatal("should be 5 now")
	}
}

func TestWaitMultipleRead(t *testing.T) {

	sobs := seqobsv.New(0)

	if sobs.Value() != 0 {
		t.Fatal("start should be 0")
	}

	ch := sobs.WaitFor(200)

	go func() {
		for {
			select {
			case <-ch:
				break
			default:
			}
			sobs.Value()
		}
	}()

	go func() {
		for {
			select {
			case <-ch:
				break
			default:
			}
			sobs.Value()
		}
	}()

	go func() {
		for {
			select {
			case <-ch:
				break
			default:
			}
			sobs.Value()
		}
	}()

	go func() {
		for i := 0; i < 201; i++ {
			sobs.Inc()
			// time.Sleep(time.Second / 100)
			// t.Log(i)
		}
	}()

	<-ch

	if sobs.Value() < 200 {
		t.Fatal("should be 200 now")
	}
}
