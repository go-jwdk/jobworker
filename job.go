package jobworker

import (
	"context"
	"sync/atomic"
)

func NewJob(queue string,
	args string,
	metadata map[string]string,
	conn Connector,
	raw interface{},
) *Job {
	return &Job{
		Queue:         queue,
		Args:          args,
		Metadata:      metadata,
		conn:          conn,
		XXX_RawMsgPtr: raw,
	}
}

type Job struct {
	Queue    string
	Args     string
	Metadata map[string]string

	LoggerFunc func(...interface{})

	conn Connector

	didSomething uint32 // atomic

	XXX_RawMsgPtr interface{} // raw message pointer
}

func (j *Job) Complete(ctx context.Context) error {
	if j.IsFinished() {
		j.debug("can't call 'Complete' on already finished job")
		return nil
	}

	_, err := j.conn.CompleteJob(ctx, &CompleteJobInput{j})
	if err != nil {
		return err
	}
	j.finished()
	return nil
}

func (j *Job) Fail(ctx context.Context) error {
	if j.IsFinished() {
		j.debug("can't call 'Fail' on already finished job")
		return nil
	}

	_, err := j.conn.FailJob(ctx, &FailJobInput{j})
	if err != nil {
		return err
	}

	j.finished()

	return nil
}

func (j *Job) IsFinished() bool {
	return atomic.LoadUint32(&j.didSomething) == 1
}

func (j *Job) SetLoggerFunc(f func(...interface{})) {
	j.LoggerFunc = f
}

func (j *Job) GetConnName() string {
	return j.conn.GetName()
}

func (j *Job) debug(msg ...interface{}) {
	if j.verbose() {
		j.LoggerFunc(msg...)
	}
}

func (j *Job) verbose() bool {
	return j.LoggerFunc != nil
}

func (j *Job) finished() {
	atomic.StoreUint32(&j.didSomething, 1)
}

// TODO remove
//func (j *Job) ExtendVisibility(ctx context.Context, visibilityTimeout int64) error {
//	if j.IsFinished() {
//		j.debug("can't call 'ExtendVisibility' on already finished job")
//		return nil
//	}
//	_, err := j.conn.ChangeJobVisibility(ctx, &ChangeJobVisibilityInput{
//		Job:               j,
//		VisibilityTimeout: visibilityTimeout,
//	})
//	if err != nil {
//		return err
//	}
//	return nil
//}
