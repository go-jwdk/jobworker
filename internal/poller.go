package internal

import (
	"context"
	"time"

	"github.com/vvatanabe/goretryer/exponential"
)

type Poller struct {
	Retryer    exponential.Retryer
	LoggerFunc func(...interface{})
}

func (p *Poller) Poll(
	interval time.Duration,
	quit <-chan struct{},
	operation func(ctx context.Context) error,
	isErrorRetryable func(err error) bool,
) {

	if p.LoggerFunc != nil {
		p.LoggerFunc = func(...interface{}) {}
	}

	for {
		select {
		case <-quit:
			return
		default:

			over, err := p.Retryer.Do(context.Background(), func(ctx context.Context) error {
				return operation(ctx)
			}, isErrorRetryable)
			if err != nil {
				if over {
					p.LoggerFunc("failed poll, retry over:", err)
				} else {
					p.LoggerFunc("failed poll:", err)
				}
			}

			// default interval
			timeout := time.After(interval)
			select {
			case <-quit:
				return
			case <-timeout:
			}
		}
	}

}
