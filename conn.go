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

	ErrJobDuplicationDetected = fmt.Errorf("job duplication detected")
)

func Register(name string, driver Driver) {
	driversMu.Lock()
	defer driversMu.Unlock()
	if driver == nil {
		panic("JWDK: Register Driver is nil")
	}
	if _, dup := name2Driver[name]; dup {
		panic("JWDK: Register called twice for Driver " + name)
	}
	name2Driver[name] = driver
}

func Open(driverName string, attrs map[string]interface{}) (Connector, error) {
	driversMu.RLock()
	driveri, ok := name2Driver[driverName]
	driversMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("JWDK: unknown driver %q (forgotten import?)", driverName)
	}
	return driveri.Open(attrs)
}

type Option struct{}

func (o *Option) ApplyOptions(opts ...func(*Option)) {
	for _, opt := range opts {
		opt(o)
	}
}

type Subscription interface {
	Active() bool
	Queue() chan *Job
	UnSubscribe() error
}

type SubscribeInput struct {
	Queue    string
	Metadata map[string]string
}

type SubscribeOutput struct {
	Subscription Subscription
}

type EnqueueInput struct {
	Queue           string
	Payload         string
	Metadata        map[string]string
	CustomAttribute map[string]*CustomAttribute
}

type EnqueueOutput struct{}

type EnqueueBatchInput struct {
	Queue           string
	Id2Content      map[string]string
	Metadata        map[string]string
	CustomAttribute map[string]*CustomAttribute
}

type EnqueueBatchOutput struct {
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

type Connector interface {
	Name() string
	Subscribe(ctx context.Context, input *SubscribeInput) (*SubscribeOutput, error)
	Enqueue(ctx context.Context, input *EnqueueInput) (*EnqueueOutput, error)
	EnqueueBatch(ctx context.Context, input *EnqueueBatchInput) (*EnqueueBatchOutput, error)
	CompleteJob(ctx context.Context, input *CompleteJobInput) (*CompleteJobOutput, error)
	FailJob(ctx context.Context, input *FailJobInput) (*FailJobOutput, error)
	Close() error
	SetLoggerFunc(f LoggerFunc)
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
