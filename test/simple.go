package test // import "cryptoscope.co/go/margaret/test"

import (
	"context"
  "fmt"
	"testing"
	"time"

  "cryptoscope.co/go/luigi"
  "cryptoscope.co/go/margaret"
)

func LogTestSimple(f func() margaret.Log) func(*testing.T) {
  type testcase struct {
    values []interface{}
    specs  []margaret.QuerySpec
    result []interface{}
    errStr string
    live bool
  }

  mkTest := func(tc testcase) func(*testing.T){
    return func(t *testing.T) {
      log := f()
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


      ctx := context.Background()
      ctx, cancel := context.WithCancel(ctx)
      defer cancel()

      waiter := make(chan struct{})
      var v_ interface{}
      err = nil

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
        if v_ != v && tc.errStr == "" {
          t.Errorf("expected %v but got %v", v, v_)
        }
        if err != nil {
          break
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
      if !tc.live && !luigi.IsEOS(err) {
        t.Errorf("expected end-of-stream but got %q", err)
      } else if tc.live && err != context.Canceled {
        t.Errorf("expected context canceled but got %q", err)
      }

      select {
      case <-time.After(time.Millisecond):
        cancel()
      case waiter <- struct{}{}:
      }
    }
  }

  tcs := []testcase{
    {
      values: []interface{}{1, 2, 3},
      result: []interface{}{1, 2, 3},
    },

    {
      values: []interface{}{1, 2, 3},
      result: []interface{}{2, 3},
      specs:  []margaret.QuerySpec{margaret.Gt(0)},
    },

    {
      values: []interface{}{1, 2, 3},
      result: []interface{}{2, 3},
      specs:  []margaret.QuerySpec{margaret.Gte(1)},
    },

    {
      values: []interface{}{1, 2, 3},
      result: []interface{}{1, 2},
      specs:  []margaret.QuerySpec{margaret.Lt(2)},
    },

    {
      values: []interface{}{1, 2, 3},
      result: []interface{}{1, 2},
      specs:  []margaret.QuerySpec{margaret.Lte(1)},
    },

    {
      values: []interface{}{1, 2, 3},
      result: []interface{}{1, 2},
      specs:  []margaret.QuerySpec{margaret.Limit(2)},
    },

    {
      values: []interface{}{1, 2},
      result: []interface{}{1, 2, 3},
      errStr: "end of stream",
    },

    {
      values: []interface{}{1, 2, 3},
      result: []interface{}{1, 2, 3},
      specs:  []margaret.QuerySpec{margaret.Live(true)},
      live:true,
    },
  }

  return func(t *testing.T) {
    for i, tc := range tcs {
      t.Run(fmt.Sprint(i), mkTest(tc))
    }
  }
}

