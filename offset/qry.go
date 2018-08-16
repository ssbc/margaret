package offset // import "go.cryptoscope.co/margaret/offset"

import (
	"context"
	"fmt"
	"runtime"
	"runtime/debug"
	"sync"

	"go.cryptoscope.co/luigi"
	"go.cryptoscope.co/margaret"

	"github.com/pkg/errors"
)

type offsetQuery struct {
	l     sync.Mutex
	log   *offsetLog
	codec margaret.Codec

	nextSeq, lt margaret.BaseSeq

	limit   int
	live    bool
	seqWrap bool
	close   chan struct{}
	err     error
}

func (qry *offsetQuery) Gt(s margaret.Seq) error {
	if qry.nextSeq > margaret.SeqEmpty {
		return errors.Errorf("lower bound already set")
	}

	qry.nextSeq = margaret.BaseSeq(s.Seq() + 1)
	return nil
}

func (qry *offsetQuery) Gte(s margaret.Seq) error {
	if qry.nextSeq > margaret.SeqEmpty {
		return errors.Errorf("lower bound already set")
	}

	qry.nextSeq = margaret.BaseSeq(s.Seq())
	return nil
}

func (qry *offsetQuery) Lt(s margaret.Seq) error {
	if qry.lt != margaret.SeqEmpty {
		return errors.Errorf("upper bound already set")
	}

	qry.lt = margaret.BaseSeq(s.Seq())
	return nil
}

func (qry *offsetQuery) Lte(s margaret.Seq) error {
	if qry.lt != margaret.SeqEmpty {
		return errors.Errorf("upper bound already set")
	}

	qry.lt = margaret.BaseSeq(s.Seq() + 1)
	return nil
}

func (qry *offsetQuery) Limit(n int) error {
	qry.limit = n
	return nil
}

func (qry *offsetQuery) Live(live bool) error {
	qry.live = live
	return nil
}

func (qry *offsetQuery) SeqWrap(wrap bool) error {
	qry.seqWrap = wrap
	return nil
}

func (qry *offsetQuery) Next(ctx context.Context) (interface{}, error) {
	qry.l.Lock()
	defer qry.l.Unlock()

	fmt.Println("qry.Next: lock taken")
	defer fmt.Println("qry.Next: releasing lock")

	if qry.limit == 0 {
		return nil, luigi.EOS{}
	}
	qry.limit--

	if qry.nextSeq == margaret.SeqEmpty {
		qry.nextSeq = 0
	}

	qry.log.l.Lock()
	defer qry.log.l.Unlock()

	seekTo := int64(qry.nextSeq) * qry.log.framing.FrameSize()

	fi, err := qry.log.f.Stat()
	if err != nil {
		return nil, errors.Wrap(err, "stat error")
	}

	if fi.Size() < seekTo+qry.log.framing.FrameSize() {
		if !qry.live {
			return nil, luigi.EOS{}
		}

		wait := make(chan struct{})
		var cancel func()
		cancel = qry.log.seq.Register(luigi.FuncSink(
			func(ctx context.Context, v interface{}, doClose bool) error {
				if doClose {
					return luigi.EOS{}
				}
				if v.(margaret.Seq).Seq() >= qry.nextSeq.Seq() {
					close(wait)
					cancel()
				}

				return nil
			}))

		err := func() error {
			qry.log.l.Unlock()
			defer qry.log.l.Lock()

			select {
			case <-wait:
			case <-ctx.Done():
				return ctx.Err()
			}
			return nil
		}()
		if err != nil {
			return nil, err
		}
	}

	if qry.lt != margaret.SeqEmpty && !(qry.nextSeq < qry.lt) {
		return nil, luigi.EOS{}
	}

	v, err := qry.log.readFrame(qry.nextSeq)
	if err != nil {
		return nil, errors.Wrap(err, "error reading next frame")
	}

	defer func() { qry.nextSeq++ }()

	if qry.seqWrap {
		return margaret.WrapWithSeq(v, qry.nextSeq), nil
	}

	return v, nil
}

func (qry *offsetQuery) Push(ctx context.Context, sink luigi.Sink) error {
	// first fast fwd's until we are up to date,
	// then hooks us into the live log updater.
	cancel, err := qry.fastFwdPush(ctx, sink)
	if err != nil {
		return errors.Wrap(err, "error in fast forward")
	}

	defer cancel()

	// block until cancelled, then clean up and return
	select {
	case <-ctx.Done():
		if qry.err != nil {
			return qry.err
		}

		return ctx.Err()
	case <-qry.close:
		return qry.err
	}
}

func (qry *offsetQuery) fastFwdPush(ctx context.Context, sink luigi.Sink) (func(), error) {
	qry.log.l.Lock()
	defer qry.log.l.Unlock()

	fmt.Println("qry.fastFwd: lock taken")
	defer fmt.Println("qry.fastFwd: releasing lock")

	if qry.nextSeq == margaret.SeqEmpty {
		qry.nextSeq = 0
	}

	// determines whether we should go on
	goon := func(seq margaret.BaseSeq) bool {
		fmt.Println("goon:")
		fmt.Println("  seq:", seq)
		fmt.Println("  qry.limit:", qry.limit)
		fmt.Println("  qry.lt:", qry.lt)

		fmt.Println("  call stack:")
		_, file, line, ok := runtime.Caller(4)
		fmt.Printf("    %s:%d ok:%v\n", file, line, ok)
		_, file, line, ok = runtime.Caller(3)
		fmt.Printf("    %s:%d ok:%v\n", file, line, ok)
		_, file, line, ok = runtime.Caller(2)
		fmt.Printf("    %s:%d ok:%v\n", file, line, ok)
		_, file, line, ok = runtime.Caller(1)
		fmt.Printf("    %s:%d ok:%v\n", file, line, ok)

		goon := qry.limit != 0 &&
			!(qry.lt >= 0 && seq >= qry.lt)
		fmt.Println("  goon:", goon)
		return goon
	}

	for goon(qry.nextSeq) {
		fmt.Println()
		fmt.Println("readFram loop top")
		qry.limit--

		// TODO: maybe don't read the frames individually but stream over them?
		//     i.e. don't use ReadAt but have a separate fd just for this query
		//     and just Read that.
		v, err := qry.log.readFrame(qry.nextSeq)
		fmt.Printf("readFrame returned %v, error: %s\n", v, err)
		if err != nil {
			// TODO dieser error verschwindet!!!1
			break
		}

		if qry.seqWrap {
			v = margaret.WrapWithSeq(v, qry.nextSeq)
		}

		err = sink.Pour(ctx, v)
		if err != nil {
			return nil, errors.Wrap(err, "error pouring read value")
		}

		qry.nextSeq++
		fmt.Println("readFram loop bottom")
	}

	fmt.Println("readFram loop end")

	if !goon(qry.nextSeq) {
		close(qry.close)
		return func() {}, sink.Close()
	}

	if !qry.live {
		close(qry.close)
		return func() {}, sink.Close()
	}

	var cancel func()
	var closed bool
	cancel = qry.log.bcast.Register(LockSink(luigi.FuncSink(func(ctx context.Context, v interface{}, doClose bool) error {
		fmt.Printf("intermediate sink pour. v: %v(%T), doClose: %v\n", v, v, doClose)
		defer fmt.Println("intermediate sink pour returns")

		if doClose {
			fmt.Println("closing qry.close because doClose. already closed:", closed)
			if closed {
				return errors.New("closing closed sink")
			}

			closed = true
			select {
			case <-qry.close:
			default:
				close(qry.close)
			}

			return errors.Wrap(sink.Close(), "error closing sink")
		}

		sw := v.(margaret.SeqWrapper)
		v, seq := sw.Value(), sw.Seq()

		if !goon(margaret.BaseSeq(seq.Seq())) {
			close(qry.close)
		}

		if qry.seqWrap {
			v = sw
		}

		return errors.Wrap(sink.Pour(ctx, v), "error pouring into sink")
	})))
	oldCancel := cancel
	cancel = func() {
		fmt.Println("bcast cancel called")
		defer fmt.Println("bcast cancel done")
		debug.PrintStack()
		oldCancel()
	}

	return func() {
		cancel()
	}, nil
}

func LockSink(sink luigi.Sink) luigi.Sink {
	var l sync.Mutex

	return luigi.FuncSink(func(ctx context.Context, v interface{}, doClose bool) error {
		l.Lock()
		defer l.Unlock()

		if doClose {
			return sink.Close()
		}

		return sink.Pour(ctx, v)
	})
}
