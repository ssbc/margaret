package margaret // import "cryptoscope.co/go/margaret"

import (
	"context"
	"testing"
	"time"

  "cryptoscope.co/go/luigi"
)

func TestMemoryLogSimple(t *testing.T) {
	type testcase struct {
		values []interface{}
		specs  []QuerySpec
		result []interface{}
		errStr string
	}

	test := func(tc testcase) {
		log := NewMemoryLog()
		for _, v := range tc.values {
			err := log.Append(v)
			if err != nil {
				t.Error("error appending:", err)
				return
			}
		}

		src, err := log.Query(tc.specs...)
		if err != nil {
			t.Error("error querying:", err)
			return
		}

		err = nil

		var v_ interface{}

		ctx := context.Background()
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		waiter := make(chan struct{})

		for _, v := range tc.result {
			go func() {
				select {
				case <-time.After(time.Millisecond):
					t.Log("canceling context")
					cancel()
				case <-waiter:
				}
			}()

			v_, err = src.Next(ctx)
			if err != nil {
				break
			}
			if v_ != v {
				t.Errorf("expected %v but got %v", v, v_)
			}
			waiter <- struct{}{}
		}

		if err != nil && tc.errStr == "" {
			t.Errorf("unexpected error %s", err)
		} else if err == nil && tc.errStr != "" {
			t.Errorf("expected error %q but got nil", tc.errStr)
		} else if tc.errStr != "" && err.Error() != tc.errStr {
			t.Errorf("expected error %q but got %q", tc.errStr, err)
		}

		go func() {
			select {
			case <-time.After(time.Millisecond):
				cancel()
			case <-waiter:
			}
		}()
		_, err = src.Next(ctx)
		live := src.(*memlogQuery).live
		if !live && !luigi.IsEOS(err) {
			t.Errorf("expected end-of-stream but got %q", err)
		} else if live && err != context.Canceled {
			t.Errorf("expected context canceled but got %q", err)
		}

		select {
		case <-time.After(time.Millisecond):
			cancel()
		case waiter <- struct{}{}:
		}
	}

	tcs := []testcase{
		{
			values: []interface{}{1, 2, 3},
			result: []interface{}{1, 2, 3},
		},
		{
			values: []interface{}{1, 2, 3},
			result: []interface{}{1, 2},
			specs:  []QuerySpec{Limit(2)},
		},
		{
			values: []interface{}{1, 2},
			result: []interface{}{1, 2, 3},
			errStr: "end of stream",
		},
		{
			values: []interface{}{1, 2, 3},
			result: []interface{}{1, 2, 3},
			specs:  []QuerySpec{Live(true)},
		},
	}

	for _, tc := range tcs {
		test(tc)
	}
}

func TestMemoryLogConcurrent(t *testing.T) {
	type testcase struct {
		values []interface{}
		specs  []QuerySpec
		result []interface{}
		errStr string
	}

	test := func(tc testcase) {
		log := NewMemoryLog()
		for _, v := range tc.values {
			err := log.Append(v)
			if err != nil {
				t.Error("error appending:", err)
				return
			}
		}

		src, err := log.Query(tc.specs...)
		if err != nil {
			t.Error("error querying:", err)
			return
		}

		err = nil

		var v_ interface{}

		ctx := context.Background()
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		waiter := make(chan struct{})

		for _, v := range tc.result {
			go func() {
				select {
				case <-time.After(time.Millisecond):
					t.Log("canceling context")
					cancel()
				case <-waiter:
				}
			}()

			v_, err = src.Next(ctx)
			if err != nil {
				break
			}
			if v_ != v {
				t.Errorf("expected %v but got %v", v, v_)
			}
			waiter <- struct{}{}
		}

		if err != nil && tc.errStr == "" {
			t.Errorf("unexpected error %s", err)
		} else if err == nil && tc.errStr != "" {
			t.Errorf("expected error %q but got nil", tc.errStr)
		} else if tc.errStr != "" && err.Error() != tc.errStr {
			t.Errorf("expected error %q but got %q", tc.errStr, err)
		}

		go func() {
			select {
			case <-time.After(time.Millisecond):
				cancel()
			case <-waiter:
			}
		}()
		_, err = src.Next(ctx)
		live := src.(*memlogQuery).live
		if !live && !luigi.IsEOS(err) {
			t.Errorf("expected end-of-stream but got %q", err)
		} else if live && err != context.Canceled {
			t.Errorf("expected context canceled but got %q", err)
		}

		select {
		case <-time.After(time.Millisecond):
			cancel()
		case waiter <- struct{}{}:
		}
	}

	tcs := []testcase{
		{
			values: []interface{}{1, 2, 3},
			result: []interface{}{1, 2, 3},
		},
		{
			values: []interface{}{1, 2, 3},
			result: []interface{}{1, 2},
			specs:  []QuerySpec{Limit(2)},
		},
		{
			values: []interface{}{1, 2},
			result: []interface{}{1, 2, 3},
			errStr: "end of stream",
		},
		{
			values: []interface{}{1, 2, 3},
			result: []interface{}{1, 2, 3},
			specs:  []QuerySpec{Live(true)},
		},
	}

	for _, tc := range tcs {
		test(tc)
	}
}

