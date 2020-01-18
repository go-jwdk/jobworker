package jobworker

import (
	"context"
	"sync/atomic"
)

type Payload struct {
	Class           string
	Args            string
	DelaySeconds    int64
	DeduplicationID string
	GroupID         string
}

func NewJob(queue string,
	jobID string,
	class string,
	receiptID string,
	args string,
	deduplicationID string,
	groupID string,
	invisibleUntil int64,
	retryCount int64,
	enqueueAt int64,
	conn Connector,
) *Job {
	return &Job{
		Queue:           queue,
		JobID:           jobID,
		Class:           class,
		ReceiptID:       receiptID,
		Args:            args,
		DeduplicationID: deduplicationID,
		GroupID:         groupID,
		InvisibleUntil:  invisibleUntil,
		RetryCount:      retryCount,
		EnqueueAt:       enqueueAt,
		conn:            conn,
	}
}

type Job struct {
	Queue string

	SecID           uint64
	JobID           string
	Class           string
	ReceiptID       string
	Args            string
	DeduplicationID string
	GroupID         string
	InvisibleUntil  int64
	RetryCount      int64
	EnqueueAt       int64

	LoggerFunc func(...interface{})

	conn Connector

	didSomething uint32 // atomic
}

func (j *Job) ExtendVisibility(ctx context.Context, visibilityTimeout int64) error {
	if j.isFinished() {
		j.debug("can't call 'ExtendVisibility' on already finished job")
		return nil
	}
	_, err := j.conn.ChangeJobVisibility(ctx, &ChangeJobVisibilityInput{
		Job:               j,
		VisibilityTimeout: visibilityTimeout,
	})
	if err != nil {
		return err
	}
	return nil
}

func (j *Job) Complete(ctx context.Context) error {
	if j.isFinished() {
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
	if j.isFinished() {
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

func (j *Job) isFinished() bool {
	return atomic.LoadUint32(&j.didSomething) == 1
}

func (j *Job) finished() {
	atomic.StoreUint32(&j.didSomething, 1)
}
