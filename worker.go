package jobworker

import "context"

type WorkerFunc func(job *Job) error

type Worker interface {
	Work(*Job) error
}

type defaultWorker struct {
	workFunc func(*Job) error
}

func (w *defaultWorker) Work(job *Job) error {
	if w.workFunc != nil {
		return w.workFunc(job)
	}
	return nil
}

type subWorker struct {
	id string
	*JobWorker
}

func (sw *subWorker) work(jobs <-chan *Job) {
	for job := range jobs {
		sw.trackJob(job, true)
		sw.WorkOnceSafely(context.Background(), job)
		sw.trackJob(job, false)
	}
}

type workerWithOption struct {
	worker Worker
	opt    *Option
}

type Option struct {
	SubscribeMetadata map[string]string
}

type OptionFunc func(*Option)

func (o *Option) ApplyOptions(opts ...OptionFunc) {
	for _, opt := range opts {
		opt(o)
	}
}

// SubscribeMetadata is metadata of subscribe func
func SubscribeMetadata(k, v string) OptionFunc {
	return func(opt *Option) {
		if opt.SubscribeMetadata == nil {
			opt.SubscribeMetadata = make(map[string]string)
		}
		opt.SubscribeMetadata[k] = v
	}
}
