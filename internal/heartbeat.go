package internal

import (
	"errors"
	"sync/atomic"
	"time"
)

type HeartBeat struct {
	cardiacArrest chan struct{}
	started       int32
}

func (jw *HeartBeat) Start(interval time.Duration, operation func()) error {

	if atomic.LoadInt32(&jw.started) == 1 {
		return errors.New("already started")
	}
	atomic.StoreInt32(&jw.started, 1)

	jw.cardiacArrest = make(chan struct{})

	go func() {
		for {
			select {
			case <-jw.cardiacArrest:
				return
			default:
				operation()

			}
			time.Sleep(interval)
		}
	}()
	return nil
}

func (jw *HeartBeat) Stop() error {
	if atomic.LoadInt32(&jw.started) == 0 {
		return errors.New("not started yet")
	}
	jw.cardiacArrest <- struct{}{}
	return nil
}
