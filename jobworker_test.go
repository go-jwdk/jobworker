package jobworker

import (
	"context"
	"errors"
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
