package jobworker

import (
	"sync/atomic"
)

func NewJob(conn Connector, queue string, payload *Payload) *Job {
	return &Job{
		conn:    conn,
		queue:   queue,
		payload: payload,
	}
}

type Job struct {
	conn    Connector
	queue   string
	payload *Payload

	didSomething uint32 // atomic
}

func (j *Job) ConnName() string {
	return j.conn.Name()
}

func (j *Job) Queue() string {
	return j.queue
}

func (j *Job) Payload() *Payload {
	return j.payload
}

func (j *Job) IsFinished() bool {
	return atomic.LoadUint32(&j.didSomething) == 1
}

func (j *Job) finished() {
	atomic.StoreUint32(&j.didSomething, 1)
}

type Payload struct {
	Content         string
	Metadata        map[string]string
	CustomAttribute map[string]*CustomAttribute
	Raw             interface{}
}

type CustomAttribute struct {
	DataType    string
	BinaryValue []byte
	StringValue string
}
