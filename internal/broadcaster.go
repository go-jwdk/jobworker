package internal

import "sync"

type Broadcaster struct {
	mu *sync.Mutex
	c  *sync.Cond
}

func (b *Broadcaster) Register(operation func()) {
	if b.c == nil {
		b.mu = new(sync.Mutex)
		b.c = sync.NewCond(b.mu)
	}
	go func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		b.c.Wait()
		operation()
	}()
}

func (b *Broadcaster) Broadcast() {
	if b.c != nil {
		b.c.Broadcast()
	}
}
