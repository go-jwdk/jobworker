package internal

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestBroadcaster_Broadcast(t *testing.T) {
	t.Run("normal case", func(t *testing.T) {
		var cnt int64
		var b Broadcaster
		for i := 0; i < 100; i++ {
			b.Register(func() {
				atomic.AddInt64(&cnt, 1)
			})
		}
		time.Sleep(time.Second)
		b.Broadcast()
		time.Sleep(time.Second)
		if cnt != 100 {
			t.Errorf("Broadcaster.Broadcast() = %v, want %v", cnt, 100)
		}
	})
}
