package tx4go

import "time"

type Option interface {
	Apply(*Manager)
}

type optionFunc func(*Manager)

func (f optionFunc) Apply(m *Manager) {
	f(m)
}

func WithTimout(timeout time.Duration) Option {
	return optionFunc(func(m *Manager) {
		m.timeout = timeout
	})
}

func WithRetryCount(count int) Option {
	return optionFunc(func(m *Manager) {
		if count <= 0 {
			count = kDefaultRetryCount
		}
		m.retryCount = count
	})
}

func WithRetryDelay(delay time.Duration) Option {
	return optionFunc(func(m *Manager) {
		m.retryDelay = delay
	})
}
