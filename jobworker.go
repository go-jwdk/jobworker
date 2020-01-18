package jobworker

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-job-worker-development-kit/jobworker/internal"
)

type Setting struct {
	Primary   Connector
	Secondary Connector

	DeadConnectorRetryInterval int64 // Seconds

	Logger Logger
}

var (
	ErrPrimaryConnIsRequired = errors.New("primary conn is required")
)

func New(s *Setting) (*JobWorker, error) {

	if s.Primary == nil {
		return nil, ErrPrimaryConnIsRequired
	}

	var w JobWorker
	w.connProvider.Register(1, s.Primary)
	if s.Secondary != nil {
		w.connProvider.Register(2, s.Secondary)
	}
	w.connProvider.SetRetrySeconds(time.Duration(s.DeadConnectorRetryInterval) * time.Second)
	w.logger = s.Logger

	return &w, nil
}

type JobWorker struct {
	connProvider ConnectorProvider

	class2worker map[string]Worker

	logger Logger

	started int32

	inShutdown  int32
	mu          sync.Mutex
	activeJob   map[*Job]struct{}
	activeJobWg sync.WaitGroup
	doneChan    chan struct{}
	onShutdown  []func()
}

type Logger interface {
	Debug(...interface{})
}

func (jw *JobWorker) EnqueueJob(ctx context.Context, input *EnqueueJobInput) error {

	for priority, conn := range jw.connProvider.GetConnectorsInPriorityOrder() {

		if jw.connProvider.IsDead(conn) {
			jw.debug("connector is dead. priority: ", priority)
			continue
		}

		_, err := conn.EnqueueJob(ctx, input)
		if err != nil {

			if err == ErrJobDuplicationDetected {
				jw.debug("skip enqueue a duplication job")
				return nil
			}
			jw.debug("mark dead connector, because could not enqueue job. priority:", priority, "err:", err)
			jw.connProvider.MarkDead(conn)
			continue
		}
		return nil
	}

	return errors.New("could not enqueue a job using all connector")
}

func (jw *JobWorker) EnqueueJobBatch(ctx context.Context, input *EnqueueJobBatchInput) error {
	for priority, conn := range jw.connProvider.GetConnectorsInPriorityOrder() {

		if jw.connProvider.IsDead(conn) {
			jw.debug("connector is dead. priority: ", priority)
			continue
		}

		output, err := conn.EnqueueJobBatch(ctx, input)

		if err == nil && output != nil && len(output.Failed) == 0 {
			return nil
		}

		jw.debug("could not enqueue job batch. priority: ", priority)
		jw.connProvider.MarkDead(conn)

		if output != nil && len(output.Failed) > 0 {
			for _, id := range output.Successful {
				delete(input.Id2Payload, id)
			}
		}
	}

	return errors.New("could not enqueue batch some jobs using all connector")
}

type WorkerFunc func(*Job) error

type Worker interface {
	Work(*Job) error
}

type defaultWorker struct {
	workFunc func(*Job) error
}

func (w *defaultWorker) Work(job *Job) error {
	return w.workFunc(job)
}

func (jw *JobWorker) RegisterFunc(class string, f WorkerFunc) bool {
	return jw.Register(class, &defaultWorker{
		workFunc: f,
	})
}

func (jw *JobWorker) Register(class string, worker Worker) bool {
	jw.mu.Lock()
	defer jw.mu.Unlock()
	if class == "" || worker == nil {
		return false
	}
	if jw.class2worker == nil {
		jw.class2worker = make(map[string]Worker)
	}
	jw.class2worker[class] = worker
	return true
}

type WorkSetting struct {
	HeartbeatInterval     int64
	OnHeartBeat           func(job *Job)
	WorkerConcurrency     int
	Queue2PollingInterval map[string]int64 // key: queue name, value; polling interval (seconds)
}

const (
	workerConcurrencyDefault = 1
)

func (s *WorkSetting) setDefaults() {
	if s.WorkerConcurrency == 0 {
		s.WorkerConcurrency = workerConcurrencyDefault
	}
	if s.Queue2PollingInterval == nil {
		s.Queue2PollingInterval = make(map[string]int64)
	}
}

var (
	ErrAlreadyStarted        = errors.New("already started")
	ErrQueueSettingsRequired = errors.New("queue settings required")
	ErrNoJob                 = errors.New("no job")
)

func (jw *JobWorker) Work(s *WorkSetting) error {

	if atomic.LoadInt32(&jw.started) == 1 {
		return ErrAlreadyStarted
	}
	atomic.StoreInt32(&jw.started, 1)

	s.setDefaults()

	if len(s.Queue2PollingInterval) == 0 {
		return ErrQueueSettingsRequired
	}

	done := jw.getDoneChan()

	if s.HeartbeatInterval > 0 && s.OnHeartBeat != nil {
		interval := time.Duration(s.HeartbeatInterval) * time.Second
		jw.debug("start heart beat - interval:", interval)
		jw.startHeartbeat(interval, done, s.OnHeartBeat)
	}

	jobCh := make(chan *Job)
	defer func() {
		close(jobCh)
	}()

	var p internal.Poller
	if jw.verbose() {
		p.LoggerFunc = jw.logger.Debug
	}
	p.Retryer.NumMaxRetries = 10

	for _, conn := range jw.connProvider.GetConnectorsInPriorityOrder() {

		for name, interval := range s.Queue2PollingInterval {

			go func(interval int64, conn Connector, queue string) {
				p.Poll(time.Duration(interval)*time.Second, done, func(ctx context.Context) error {
					return jw.receiveJobsForWorkers(ctx, conn, queue, jobCh)
				}, func(err error) bool {
					return err == ErrNoJob
				})
			}(interval, conn, name)

		}
	}

	var wg sync.WaitGroup
	for i := 0; i < s.WorkerConcurrency; i++ {
		wg.Add(1)
		go func(id int) {
			sw := subWorker{id: strconv.Itoa(id), JobWorker: jw}
			sw.work(jobCh)
			wg.Done()
		}(i)
	}

	wg.Wait()

	return nil

}

type subWorker struct {
	id string
	*JobWorker
}

func (sw *subWorker) work(jobs <-chan *Job) {
	for job := range jobs {
		sw.workSafely(context.Background(), job)
	}
}

func (jw *JobWorker) workSafely(ctx context.Context, j *Job) {

	jw.debug("start work safely:", j.GetConnName(), j.Class, j.JobID)

	j.SetLoggerFunc(jw.debug)

	jw.trackJob(j, true)
	defer jw.trackJob(j, false)

	w, ok := jw.class2worker[j.Class]
	if !ok {
		jw.debug("could not found class:", j.Class)
		return
	}

	if err := w.Work(j); err != nil {
		if err = j.Fail(ctx); err != nil {
			jw.debug("mark dead connector, because error occurred during job fail:", j.conn.GetName(), j.Class, j.JobID, err)
			jw.connProvider.MarkDead(j.conn)
		}
		return
	}
	if err := j.Complete(ctx); err != nil {
		jw.debug("mark dead connector, because error occurred during job complete:", j.conn.GetName(), j.Class, j.JobID, err)
		jw.connProvider.MarkDead(j.conn)
		return
	}
	jw.debug("success work safely:", j.Class, j.JobID)
}

func (jw *JobWorker) receiveJobsForWorkers(ctx context.Context, conn Connector, queue string, ch chan<- *Job) error {
	output, err := conn.ReceiveJobs(ctx, ch, &ReceiveJobsInput{
		Queue: queue,
	})
	if err != nil {
		return err
	}
	if output.NoJob {
		return ErrNoJob
	}
	return nil
}

func (jw *JobWorker) RegisterOnShutdown(f func()) {
	jw.mu.Lock()
	jw.onShutdown = append(jw.onShutdown, f)
	jw.mu.Unlock()
}

func (jw *JobWorker) Shutdown(ctx context.Context) error {
	atomic.StoreInt32(&jw.inShutdown, 1)

	jw.mu.Lock()
	jw.closeDoneChanLocked()
	for _, f := range jw.onShutdown {
		go f()
	}
	jw.mu.Unlock()

	finished := make(chan struct{}, 1)
	go func() {
		jw.activeJobWg.Wait()
		finished <- struct{}{}
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-finished:
		return nil
	}
}

const logPrefix = "[jwdk]"

func (jw *JobWorker) debug(args ...interface{}) {
	if jw.verbose() {
		args = append([]interface{}{logPrefix}, args...)
		jw.logger.Debug(args...)
	}
}

func (jw *JobWorker) verbose() bool {
	return jw.logger != nil
}

func (jw *JobWorker) startHeartbeat(interval time.Duration, done <-chan struct{}, f func(job *Job)) {
	go func() {
	L:
		for {

			select {
			case <-done:
				break L
			default:

				var jobs []*Job
				jw.mu.Lock()
				for v := range jw.activeJob {
					jobs = append(jobs, v)
				}
				jw.mu.Unlock()

				go func(jobs []*Job) {
					for _, job := range jobs {
						f(job)
					}
				}(jobs)

			}
			time.Sleep(interval)
		}
	}()
}

func (jw *JobWorker) shuttingDown() bool {
	return atomic.LoadInt32(&jw.inShutdown) != 0
}

func (jw *JobWorker) trackJob(j *Job, add bool) {
	jw.mu.Lock()
	defer jw.mu.Unlock()
	if jw.activeJob == nil {
		jw.activeJob = make(map[*Job]struct{})
	}
	if add {
		jw.activeJob[j] = struct{}{}
		jw.activeJobWg.Add(1)
	} else {
		delete(jw.activeJob, j)
		jw.activeJobWg.Done()
	}
}

func (jw *JobWorker) getDoneChan() <-chan struct{} {
	jw.mu.Lock()
	defer jw.mu.Unlock()
	return jw.getDoneChanLocked()
}

func (jw *JobWorker) getDoneChanLocked() chan struct{} {
	if jw.doneChan == nil {
		jw.doneChan = make(chan struct{})
	}
	return jw.doneChan
}

func (jw *JobWorker) closeDoneChanLocked() {
	ch := jw.getDoneChanLocked()
	select {
	case <-ch:
	default:
		close(ch)
	}
}
