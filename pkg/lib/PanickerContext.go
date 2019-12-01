package lib

import (
	"context"
	"sync"
	"time"
)

var Panicked error = panickedError{}

type panickedError struct{}

func (panickedError) Error() string { return "context panicked" }

type PanicCancelFun func() *sync.WaitGroup

type PanickerContext struct {
	context  context.Context
	canFn    func()
	panicked bool

	panicCh chan error

	runningWg *sync.WaitGroup
}

func WithPanic(parentCtx context.Context) (*PanickerContext, PanicCancelFun) {
	ctx, canFn := context.WithCancel(parentCtx)
	return withPanic(parentCtx, ctx, canFn)
}

/*
func WithPanicAndTimeout(parentCtx context.Context, timeout time.Duration) (*PanickerContext, PanicCancelFun) {
	ctx, canFn := context.WithTimeout(parentCtx, timeout)
	return withPanic(parentCtx, ctx, canFn)
}
*/

func withPanic(parentCtx context.Context, baseCtx context.Context, canFn context.CancelFunc) (*PanickerContext, PanicCancelFun) {
	p, ok := parentCtx.(*PanickerContext)

	var newCtx *PanickerContext
	if ok {
		newCtx = &PanickerContext{
			context:   baseCtx,
			canFn:     canFn,
			panicked:  false,
			panicCh:   p.panicCh,
			runningWg: p.runningWg,
		}

		newCtx.AddRunning()
	} else {
		newCtx = &PanickerContext{
			context:   baseCtx,
			canFn:     canFn,
			panicked:  false,
			panicCh:   make(chan error),
			runningWg: new(sync.WaitGroup),
		}
	}

	return newCtx, func() *sync.WaitGroup { return newCtx.cancel() }
}

func (p *PanickerContext) cancel() *sync.WaitGroup {
	p.canFn()
	return p.runningWg
}

func (p *PanickerContext) AddRunning() {
	p.runningWg.Add(1)
}

func (p *PanickerContext) Finish() {
	p.runningWg.Done()
}

func (p *PanickerContext) Panic(err error) {
	p.canFn()
	p.panicked = true
	p.panicCh <- err
}

func (p *PanickerContext) Panicked() chan error {
	return p.panicCh
}

func (p *PanickerContext) Done() <-chan struct{} {
	return p.context.Done()
}

func (p *PanickerContext) Err() error {
	if p.panicked {
		return Panicked
	}
	return p.context.Err()
}

func (*PanickerContext) Deadline() (deadline time.Time, ok bool) {
	return
}

func (*PanickerContext) Value(key interface{}) interface{} {
	return nil
}
