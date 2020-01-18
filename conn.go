package jobworker

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

type Driver interface {
	Open(attrs map[string]interface{}) (Connector, error)
}

var (
	driversMu   sync.RWMutex
	name2Driver = make(map[string]Driver)
)

func Register(name string, driver Driver) {
	driversMu.Lock()
	defer driversMu.Unlock()
	if driver == nil {
		panic("jwdk: Register Driver is nil")
	}
	if _, dup := name2Driver[name]; dup {
		panic("jwdk: Register called twice for Driver " + name)
	}
	name2Driver[name] = driver
}

func Open(driverName string, attrs map[string]interface{}) (Connector, error) {
	driversMu.RLock()
	driveri, ok := name2Driver[driverName]
	driversMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("jwdk: unknown driver %q (forgotten import?)", driverName)
	}
	return driveri.Open(attrs)
}

type ReceiveJobsInput struct {
	Queue string
}

type ReceiveJobsOutput struct {
	NoJob bool
}

type EnqueueJobInput struct {
	Queue   string
	Payload *Payload
}

type EnqueueJobOutput struct {
	JobID string
}

type Option struct {
}

func (o *Option) ApplyOptions(opts ...func(*Option)) {
	for _, opt := range opts {
		opt(o)
	}
}

var ErrJobDuplicationDetected = fmt.Errorf("job duplication detected")

type EnqueueJobBatchInput struct {
	Queue      string
	Id2Payload map[string]*Payload
}

type EnqueueJobBatchOutput struct {
	Failed     []string // ID
	Successful []string // ID
}

type CompleteJobInput struct {
	Job *Job
}

type CompleteJobOutput struct{}

type FailJobInput struct {
	Job *Job
}

type FailJobOutput struct{}

type ChangeJobVisibilityInput struct {
	Job               *Job
	VisibilityTimeout int64
}

type ChangeJobVisibilityOutput struct{}

type CreateQueueInput struct {
	Name       string
	Attributes map[string]interface{}
}

type CreateQueueOutput struct{}

type UpdateQueueInput struct {
	Name       string
	Attributes map[string]interface{}
}

type UpdateQueueOutput struct{}

type RedriveJobInput struct {
	From         string
	To           string
	Target       string
	DelaySeconds int64
}

type RedriveJobOutput struct{}

type Connector interface {
	GetName() string

	ReceiveJobs(ctx context.Context, ch chan<- *Job, input *ReceiveJobsInput, opts ...func(*Option)) (*ReceiveJobsOutput, error)
	EnqueueJob(ctx context.Context, input *EnqueueJobInput, opts ...func(*Option)) (*EnqueueJobOutput, error)
	EnqueueJobBatch(ctx context.Context, input *EnqueueJobBatchInput, opts ...func(*Option)) (*EnqueueJobBatchOutput, error)
	CompleteJob(ctx context.Context, input *CompleteJobInput, opts ...func(*Option)) (*CompleteJobOutput, error)
	FailJob(ctx context.Context, input *FailJobInput, opts ...func(*Option)) (*FailJobOutput, error)
	ChangeJobVisibility(ctx context.Context, input *ChangeJobVisibilityInput, opts ...func(*Option)) (*ChangeJobVisibilityOutput, error)

	CreateQueue(ctx context.Context, input *CreateQueueInput, opts ...func(*Option)) (*CreateQueueOutput, error)
	UpdateQueue(ctx context.Context, input *UpdateQueueInput, opts ...func(*Option)) (*UpdateQueueOutput, error)

	RedriveJob(ctx context.Context, input *RedriveJobInput, opts ...func(*Option)) (*RedriveJobOutput, error)

	Close() error

	SetLogger(logger Logger)
}

type ConnectorProvider struct {
	retrySeconds time.Duration

	mu          sync.RWMutex
	deadRetryAt map[Connector]time.Time

	priority2Conn map[int]Connector
}

func (p *ConnectorProvider) SetRetrySeconds(sec time.Duration) {
	p.retrySeconds = sec
}

func (p *ConnectorProvider) Register(priority int, conn Connector) {
	p.mu.Lock()
	if p.priority2Conn == nil {
		p.priority2Conn = make(map[int]Connector)
	}
	p.priority2Conn[priority] = conn
	p.mu.Unlock()
}

func (p *ConnectorProvider) GetConnectorsInPriorityOrder() []Connector {
	keys := make([]int, 0, len(p.priority2Conn))

	p.mu.Lock()
	for k := range p.priority2Conn {
		keys = append(keys, k)
	}
	p.mu.Unlock()

	sort.Ints(keys)

	var sorted []Connector
	for _, k := range keys {
		sorted = append(sorted, p.priority2Conn[k])
	}

	return sorted
}

func (p *ConnectorProvider) MarkDead(conn Connector) {
	p.mu.Lock()

	if p.deadRetryAt == nil {
		p.deadRetryAt = make(map[Connector]time.Time)
	}

	p.deadRetryAt[conn] = time.Now().Add(p.retrySeconds)
	p.mu.Unlock()
}

func (p *ConnectorProvider) IsDead(conn Connector) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.deadRetryAt == nil {
		p.deadRetryAt = make(map[Connector]time.Time)
	}

	if retryAt, dead := p.deadRetryAt[conn]; dead {
		if retryAt.Before(time.Now()) {
			delete(p.deadRetryAt, conn)
			return false
		}
		return true
	}
	return false
}

func (p *ConnectorProvider) Close() {
	for _, conn := range p.priority2Conn {
		_ = conn.Close()
	}
}
