package jobworker

import (
	"sync/atomic"
)

func NewJob(conn Connector, queueName string, payload *Payload) *Job {
	return &Job{
		conn:      conn,
		queueName: queueName,
		payload:   payload,
	}
}

type Job struct {
	conn      Connector
	queueName string
	payload   *Payload

	didSomething uint32 // atomic
}

func (j *Job) ConnName() string {
	return j.conn.Name()
}

func (j *Job) QueueName() string {
	return j.queueName
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
	Raw             interface{} // raw data of different jobs for each connector
}

type CustomAttribute struct {
	DataType    string
	BinaryValue []byte
	StringValue string
}
