package tx4go

import "time"

type Option interface {
	Apply(*txManager)
}

type optionFunc func(*txManager)

func (f optionFunc) Apply(m *txManager) {
	f(m)
}

func WithTimeout(timeout time.Duration) Option {
	return optionFunc(func(m *txManager) {
		m.timeout = timeout
	})
}

func WithRetryCount(count int) Option {
	return optionFunc(func(m *txManager) {
		if count <= 0 {
			count = kDefaultRetryCount
		}
		m.retryCount = count
	})
}

func WithRetryDelay(delay time.Duration) Option {
	return optionFunc(func(m *txManager) {
		m.retryDelay = delay
	})
}
