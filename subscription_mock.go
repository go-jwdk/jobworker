package jobworker

type SubscriptionMock struct {
	ActiveFunc      func() bool
	QueueFunc       func() chan *Job
	UnSubscribeFunc func() error
}

func (m *SubscriptionMock) Active() bool {
	if m.ActiveFunc == nil {
		panic("This method is not defined.")
	}
	return m.ActiveFunc()
}

func (m *SubscriptionMock) Queue() chan *Job {
	if m.QueueFunc == nil {
		panic("This method is not defined.")
	}
	return m.QueueFunc()
}

func (m *SubscriptionMock) UnSubscribe() error {
	if m.UnSubscribeFunc == nil {
		panic("This method is not defined.")
	}
	return m.UnSubscribeFunc()
}
