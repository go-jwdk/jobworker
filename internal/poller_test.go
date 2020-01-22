package internal

import (
	"context"
	"testing"
	"time"
)

func TestPoll(t *testing.T) {
	quit := make(chan struct{})
	go func() {
		time.Sleep(time.Second)
		close(quit)
	}()
	var (
		poller Poller
		val    int
	)
	go func() {
		poller.Start(time.Second, quit, func(ctx context.Context) error {
			val++
			return nil
		}, func(err error) bool {
			return false
		})
	}()

	time.Sleep(time.Millisecond * 200)

	want := 1
	if val != want {
		t.Fatalf("Start() = %v, want %v", val, want)
		return
	}
}
