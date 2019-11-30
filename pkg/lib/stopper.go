package lib

import (
	"sync"
	"sync/atomic"
)

type Stopper struct {
	cond *sync.Cond
	m    *sync.Mutex
	num  int32
}

func NewStopper() *Stopper {
	m := new(sync.Mutex)
	return &Stopper{
		m:    m,
		cond: sync.NewCond(m),
	}
}

func (s *Stopper) Stop() {
	atomic.StoreInt32(&s.num, 1)
}

func (s *Stopper) Start() {
	atomic.StoreInt32(&s.num, 0)
	s.cond.Broadcast()
}

func (s *Stopper) WaitIfNeeded() {
	if atomic.LoadInt32(&s.num) == 1 {
		s.m.Lock()
		defer s.m.Unlock()
		s.cond.Wait()
	}
}
