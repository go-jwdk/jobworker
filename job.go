package jobworker

import (
	"sync/atomic"
)

type Job struct {
	Conn            Connector
	QueueName       string
	Content         string
	Metadata        map[string]string
	CustomAttribute map[string]*CustomAttribute
	Raw             interface{} // raw data of different jobs for each connector
	didSomething    uint32      // atomic
}

func (j *Job) IsFinished() bool {
	return atomic.LoadUint32(&j.didSomething) == 1
}

func (j *Job) finished() {
	atomic.StoreUint32(&j.didSomething, 1)
}

type CustomAttribute struct {
	DataType    string
	BinaryValue []byte
	StringValue string
}
