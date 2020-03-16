package jobworker

import (
	"context"
)

type ConnectorMock struct {
	NameFunc          func() string
	SubscribeFunc     func(ctx context.Context, input *SubscribeInput) (*SubscribeOutput, error)
	EnqueueFunc       func(ctx context.Context, input *EnqueueInput) (*EnqueueOutput, error)
	EnqueueBatchFunc  func(ctx context.Context, input *EnqueueBatchInput) (*EnqueueBatchOutput, error)
	CompleteJobFunc   func(ctx context.Context, input *CompleteJobInput) (*CompleteJobOutput, error)
	FailJobFunc       func(ctx context.Context, input *FailJobInput) (*FailJobOutput, error)
	CloseFunc         func() error
	SetLoggerFuncFunc func(f LoggerFunc)
}

func (m *ConnectorMock) Name() string {
	if m.NameFunc == nil {
		panic("This method is not defined.")
	}
	return m.NameFunc()
}

func (m *ConnectorMock) Subscribe(ctx context.Context, input *SubscribeInput) (*SubscribeOutput, error) {
	if m.SubscribeFunc == nil {
		panic("This method is not defined.")
	}
	return m.SubscribeFunc(ctx, input)
}

func (m *ConnectorMock) Enqueue(ctx context.Context, input *EnqueueInput) (*EnqueueOutput, error) {
	if m.EnqueueFunc == nil {
		panic("This method is not defined.")
	}
	return m.EnqueueFunc(ctx, input)
}

func (m *ConnectorMock) EnqueueBatch(ctx context.Context, input *EnqueueBatchInput) (*EnqueueBatchOutput, error) {
	if m.EnqueueBatchFunc == nil {
		panic("This method is not defined.")
	}
	return m.EnqueueBatchFunc(ctx, input)
}

func (m *ConnectorMock) CompleteJob(ctx context.Context, input *CompleteJobInput) (*CompleteJobOutput, error) {
	if m.CompleteJobFunc == nil {
		panic("This method is not defined.")
	}
	return m.CompleteJobFunc(ctx, input)
}

func (m *ConnectorMock) FailJob(ctx context.Context, input *FailJobInput) (*FailJobOutput, error) {
	if m.FailJobFunc == nil {
		panic("This method is not defined.")
	}
	return m.FailJobFunc(ctx, input)
}

func (m *ConnectorMock) Close() error {
	if m.CloseFunc == nil {
		panic("This method is not defined.")
	}
	return m.CloseFunc()
}

func (m *ConnectorMock) SetLoggerFunc(f LoggerFunc) {
	if m.SetLoggerFuncFunc == nil {
		panic("This method is not defined.")
	}
	m.SetLoggerFuncFunc(f)
}
