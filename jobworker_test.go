package jobworker

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"
)

func Test_completeJob(t *testing.T) {
	type args struct {
		ctx context.Context
		job *Job
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "normal case",
			args: args{
				ctx: context.Background(),
				job: &Job{
					Conn: &ConnectorMock{
						CompleteJobFunc: func(ctx context.Context, input *CompleteJobInput) (output *CompleteJobOutput, e error) {
							return &CompleteJobOutput{}, nil
						},
					},
					QueueName:    "Foo",
					didSomething: 0,
				},
			},
			wantErr: false,
		},
		{
			name: "error case",
			args: args{
				ctx: context.Background(),
				job: &Job{
					Conn: &ConnectorMock{
						CompleteJobFunc: func(ctx context.Context, input *CompleteJobInput) (output *CompleteJobOutput, e error) {
							return nil, errors.New("mock error")
						},
					},
					QueueName:    "Foo",
					didSomething: 0,
				},
			},
			wantErr: true,
		},
		{
			name: "already finish",
			args: args{
				ctx: context.Background(),
				job: &Job{
					QueueName:    "Foo",
					didSomething: 1,
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := completeJob(tt.args.ctx, tt.args.job); (err != nil) != tt.wantErr {
				t.Errorf("completeJob() error = %v, wantErr %v", err, tt.wantErr)
			}
			if ok := tt.args.job.IsFinished(); ok != !tt.wantErr {
				t.Errorf("IsFinished() result = %v, wantErr %v", ok, !tt.wantErr)
			}
		})
	}
}

func Test_failJob(t *testing.T) {
	type args struct {
		ctx context.Context
		job *Job
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "normal case",
			args: args{
				ctx: context.Background(),
				job: &Job{
					Conn: &ConnectorMock{
						FailJobFunc: func(ctx context.Context, input *FailJobInput) (output *FailJobOutput, e error) {
							return &FailJobOutput{}, nil
						},
					},
					QueueName:    "Foo",
					didSomething: 0,
				},
			},
			wantErr: false,
		},
		{
			name: "error case",
			args: args{
				ctx: context.Background(),
				job: &Job{
					Conn: &ConnectorMock{
						FailJobFunc: func(ctx context.Context, input *FailJobInput) (output *FailJobOutput, e error) {
							return &FailJobOutput{}, errors.New("mock error")
						},
					},
					QueueName:    "Foo",
					didSomething: 0,
				},
			},
			wantErr: true,
		},
		{
			name: "already finish",
			args: args{
				ctx: context.Background(),
				job: &Job{
					QueueName:    "Foo",
					didSomething: 1,
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := failJob(tt.args.ctx, tt.args.job); (err != nil) != tt.wantErr {
				t.Errorf("failJob() error = %v, wantErr %v", err, tt.wantErr)
			}
			if ok := tt.args.job.IsFinished(); ok != !tt.wantErr {
				t.Errorf("IsFinished() result = %v, wantErr %v", ok, !tt.wantErr)
			}
		})
	}
}

func TestJobWorker_newActiveJobHandlerFunc(t *testing.T) {
	jw := &JobWorker{
		activeJob: make(map[*Job]struct{}),
	}
	jw.activeJob[&Job{}] = struct{}{}
	jw.activeJob[&Job{}] = struct{}{}
	jw.activeJob[&Job{}] = struct{}{}

	var cnt int
	jw.newActiveJobHandlerFunc(func(job *Job) {
		cnt++
		if _, ok := jw.activeJob[job]; !ok {
			t.Errorf("JobWorker.newActiveJobHandlerFunc() unknown job")
		}
	})()
	time.Sleep(time.Second / 2)
	if cnt != 3 {
		t.Errorf("JobWorker.newActiveJobHandlerFunc() cnt = %v, want %v", cnt, 3)
	}
}

func TestJobWorker_WorkOnceSafely(t *testing.T) {

	conn := &ConnectorMock{
		NameFunc: func() string {
			return "test"
		},
		CompleteJobFunc: func(ctx context.Context, input *CompleteJobInput) (output *CompleteJobOutput, e error) {
			if input.Job.Content == "" {
				return nil, errors.New("dummy")
			}
			return &CompleteJobOutput{}, nil
		},
		FailJobFunc: func(ctx context.Context, input *FailJobInput) (output *FailJobOutput, e error) {
			if input.Job.Content == "" {
				return nil, errors.New("dummy")
			}
			return &FailJobOutput{}, nil
		},
	}

	type fields struct {
		queue2worker map[string]*workerWithOption
		conn         Connector
		loggerFunc   LoggerFunc
	}
	type args struct {
		job *Job
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		wantConnDie bool
	}{
		{
			name: "not found worker",
			fields: fields{
				queue2worker: map[string]*workerWithOption{},
				conn:         conn,
			},
			args: args{
				job: &Job{
					Conn:      conn,
					QueueName: "foo",
					Content:   "hello",
				},
			},
			wantConnDie: false,
		},
		{
			name: "not found worker and fail is error",
			fields: fields{
				queue2worker: map[string]*workerWithOption{},
				conn:         conn,
			},
			args: args{
				job: &Job{
					Conn:      conn,
					QueueName: "foo",
				},
			},
			wantConnDie: true,
		},
		{
			name: "work is successful",
			fields: fields{
				queue2worker: map[string]*workerWithOption{
					"foo": {
						worker: &defaultWorker{},
					},
				},
				conn: conn,
			},
			args: args{
				job: &Job{
					Conn:      conn,
					QueueName: "foo",
					Content:   "hello",
				},
			},
			wantConnDie: false,
		},
		{
			name: "work is successful and complete is error",
			fields: fields{
				queue2worker: map[string]*workerWithOption{
					"foo": {
						worker: &defaultWorker{},
					},
				},
				conn: conn,
			},
			args: args{
				job: &Job{
					Conn:      conn,
					QueueName: "foo",
				},
			},
			wantConnDie: true,
		},
		{
			name: "work is failed and fail is success",
			fields: fields{
				queue2worker: map[string]*workerWithOption{
					"foo": {
						worker: &defaultWorker{
							func(job *Job) error {
								return errors.New("dummy")
							},
						},
					},
				},
				conn: conn,
			},
			args: args{
				job: &Job{
					Conn:      conn,
					QueueName: "foo",
					Content:   "hello",
				},
			},
			wantConnDie: false,
		},
		{
			name: "work is failed and fail is failed",
			fields: fields{
				queue2worker: map[string]*workerWithOption{
					"foo": {
						worker: &defaultWorker{
							func(job *Job) error {
								return errors.New("dummy")
							},
						},
					},
				},
				conn: conn,
			},
			args: args{
				job: &Job{
					Conn:      conn,
					QueueName: "foo",
				},
			},
			wantConnDie: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			jw := &JobWorker{
				queue2worker: tt.fields.queue2worker,
				loggerFunc:   tt.fields.loggerFunc,
			}

			jw.connProvider.SetRetrySeconds(time.Second)
			jw.connProvider.Register(1, tt.fields.conn)

			jw.WorkOnceSafely(context.Background(), tt.args.job)

			if v := jw.connProvider.IsDead(tt.fields.conn); v != tt.wantConnDie {
				t.Errorf("JobWorker.WorkOnceSafely() v=%v want=%v", v, tt.wantConnDie)
			}
		})
	}
}

func newSub(conn Connector, jobSize int) Subscription {
	jobs := make(chan *Job, jobSize)
	queue := make(chan *Job)
	sub := &SubscriptionMock{
		ActiveFunc: func() bool {
			return true
		},
		QueueFunc: func() chan *Job {
			return queue
		},
		UnSubscribeFunc: func() error {
			close(jobs)
			return nil
		},
	}
	go func() {
		for i := 0; i < jobSize; i++ {
			jobs <- &Job{
				Conn:      conn,
				QueueName: "foo",
				Content:   fmt.Sprintf("hello %d", i),
			}
		}
	}()
	go func() {
		for job := range jobs {
			queue <- job
		}
	}()
	return sub
}

func TestJobWorker_Main(t *testing.T) {

	var conn *ConnectorMock
	conn = &ConnectorMock{
		NameFunc: func() string {
			return "test"
		},
		SubscribeFunc: func(ctx context.Context, input *SubscribeInput) (output *SubscribeOutput, e error) {
			return &SubscribeOutput{Subscription: newSub(conn, 3)}, nil
		},
		CompleteJobFunc: func(ctx context.Context, input *CompleteJobInput) (output *CompleteJobOutput, e error) {
			return &CompleteJobOutput{}, nil
		},
		FailJobFunc: func(ctx context.Context, input *FailJobInput) (output *FailJobOutput, e error) {
			return &FailJobOutput{}, nil
		},
		CloseFunc: func() error {
			return nil
		},
	}

	var conn2 *ConnectorMock
	conn2 = &ConnectorMock{
		NameFunc: func() string {
			return "test"
		},
		SubscribeFunc: func(ctx context.Context, input *SubscribeInput) (output *SubscribeOutput, e error) {
			return &SubscribeOutput{Subscription: newSub(conn2, 3)}, nil
		},
		CompleteJobFunc: func(ctx context.Context, input *CompleteJobInput) (output *CompleteJobOutput, e error) {
			return &CompleteJobOutput{}, nil
		},
		FailJobFunc: func(ctx context.Context, input *FailJobInput) (output *FailJobOutput, e error) {
			return &FailJobOutput{}, nil
		},
		CloseFunc: func() error {
			return nil
		},
	}

	jw, err := New(&Setting{
		Primary:   conn,
		Secondary: conn2,
	})
	if err != nil {
		t.Errorf("JobWorker.New() error = %v", err)
		return
	}

	jw.Register("foo", &defaultWorker{
		func(job *Job) error {
			jw.debug("[test] content:", job.Content)
			return nil
		},
	}, SubscribeMetadata("X-JWDK-KEY", "DEADBEEF"))

	jw.RegisterFunc("bar", func(job *Job) error {
		jw.debug("[test] content:", job.Content)
		return nil
	}, SubscribeMetadata("X-JWDK-KEY", "DEADBEEF"))

	jw.RegisterOnShutdown(func() {
		jw.debug("[test] call OnShutdown func")
	})

	go func() {
		if err := jw.Work(&WorkSetting{
			HeartbeatInterval: 1,
			OnHeartBeat:       func(job *Job) {},
		}); err != nil {
			t.Errorf("JobWorker.Work() error = %v", err)
		}
	}()
	time.Sleep(time.Second)
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := jw.Shutdown(ctx); err != nil {
		t.Errorf("JobWorker.Shutdown() error = %v", err)
	}
}

func TestJobWorker_Enqueue(t *testing.T) {
	conn := &ConnectorMock{
		NameFunc: func() string {
			return "test"
		},
		EnqueueFunc: func(ctx context.Context, input *EnqueueInput) (output *EnqueueOutput, e error) {
			if input.Content == "" {
				return nil, errors.New("dummy")
			}
			if input.Content == "duplication" {
				return nil, ErrJobDuplicationDetected
			}
			return &EnqueueOutput{}, nil
		},
	}
	conn2 := &ConnectorMock{
		NameFunc: func() string {
			return "test"
		},
		EnqueueFunc: func(ctx context.Context, input *EnqueueInput) (output *EnqueueOutput, e error) {
			return &EnqueueOutput{}, nil
		},
	}
	type args struct {
		input *EnqueueInput
	}
	tests := []struct {
		name    string
		jw      *JobWorker
		args    args
		want    *EnqueueOutput
		wantErr bool
	}{
		{
			name: "normal case",
			jw: func() *JobWorker {
				j, _ := New(&Setting{
					Primary: conn,
				})
				return j
			}(),
			args: args{
				input: &EnqueueInput{
					Queue:   "foo",
					Content: "hello",
				},
			},
			wantErr: false,
		},
		{
			name:    "no active conn",
			jw:      &JobWorker{},
			args:    args{},
			wantErr: true,
		},
		{
			name: "job duplication detected",
			jw: func() *JobWorker {
				j, _ := New(&Setting{
					Primary: conn,
				})
				return j
			}(),
			args: args{
				input: &EnqueueInput{
					Queue:   "foo",
					Content: "duplication",
				},
			},
			wantErr: false,
		},
		{
			name: "unknown error",
			jw: func() *JobWorker {
				j, _ := New(&Setting{
					Primary: conn,
				})
				return j
			}(),
			args: args{
				input: &EnqueueInput{
					Queue:   "foo",
					Content: "",
				},
			},
			wantErr: true,
		},
		{
			name: "success secondary",
			jw: func() *JobWorker {
				j, _ := New(&Setting{
					Primary:   conn,
					Secondary: conn2,
				})
				return j
			}(),
			args: args{
				input: &EnqueueInput{
					Queue:   "foo",
					Content: "",
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.jw.Enqueue(context.Background(), tt.args.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("JobWorker.Enqueue() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func TestJobWorker_EnqueueBatch(t *testing.T) {
	conn := &ConnectorMock{
		NameFunc: func() string {
			return "test"
		},
		EnqueueBatchFunc: func(ctx context.Context, input *EnqueueBatchInput) (output *EnqueueBatchOutput, e error) {

			if input.Queue == "" {
				return nil, errors.New("dummy")
			}

			var successful []string
			for _, v := range input.Entries {
				successful = append(successful, v.ID)
			}
			return &EnqueueBatchOutput{
				Successful: successful,
			}, nil
		},
	}
	//conn2 := &ConnectorMock{
	//	NameFunc: func() string {
	//		return "test"
	//	},
	//	EnqueueBatchFunc: func(ctx context.Context, input *EnqueueBatchInput) (output *EnqueueBatchOutput, e error) {
	//		return &EnqueueBatchOutput{}, nil
	//	},
	//}
	type args struct {
		input *EnqueueBatchInput
	}
	tests := []struct {
		name    string
		jw      *JobWorker
		args    args
		want    *EnqueueBatchOutput
		wantErr bool
	}{
		{
			name: "normal case",
			jw: func() *JobWorker {
				j, _ := New(&Setting{
					Primary: conn,
				})
				return j
			}(),
			args: args{
				input: &EnqueueBatchInput{
					Queue: "foo",
					Entries: []*EnqueueBatchEntry{
						{
							ID:      "a-1",
							Content: "hello",
						},
						{
							ID:      "a-2",
							Content: "hello",
						},
						{
							ID:      "a-3",
							Content: "hello",
						},
					},
				},
			},
			want: &EnqueueBatchOutput{
				Successful: []string{"a-1", "a-2", "a-3"},
			},
			wantErr: false,
		},
		{
			name:    "no active conn",
			jw:      &JobWorker{},
			args:    args{},
			wantErr: true,
		},
		{
			name: "duplicate entry id",
			jw: func() *JobWorker {
				j, _ := New(&Setting{
					Primary: conn,
				})
				return j
			}(),
			args: args{
				input: &EnqueueBatchInput{
					Queue: "foo",
					Entries: []*EnqueueBatchEntry{
						{
							ID:      "a-1",
							Content: "hello",
						},
						{
							ID:      "a-1",
							Content: "hello",
						},
						{
							ID:      "a-3",
							Content: "hello",
						},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "error case",
			jw: func() *JobWorker {
				j, _ := New(&Setting{
					Primary: conn,
				})
				return j
			}(),
			args: args{
				input: &EnqueueBatchInput{
					Queue: "",
					Entries: []*EnqueueBatchEntry{
						{
							ID:      "a-1",
							Content: "hello",
						},
					},
				},
			},
			want: &EnqueueBatchOutput{
				Failed: []string{"a-1"},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.jw.EnqueueBatch(context.Background(), tt.args.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("JobWorker.EnqueueBatch() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("JobWorker.EnqueueBatch() got = %v, want %v", got, tt.want)
			}
		})
	}
}
