package internal

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestHeartBeat(t *testing.T) {
	t.Run("normal case", func(t *testing.T) {
		var cnt int64
		var hb HeartBeat
		_ = hb.Start(time.Millisecond*100, func() {
			atomic.AddInt64(&cnt, 1)
		})
		time.Sleep(time.Second)
		_ = hb.Stop()
		if cnt < 5 {
			t.Errorf("Broadcaster.Broadcast() = %v, want %v", cnt, "cnt < 5")
		}
	})
}
