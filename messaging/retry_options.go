package zaws

import "time"

const DefaultWaitBase = 1 * time.Second
const DefaultAttempts = 3
const DefaultBackoff = false

type RetryConfig struct {
	Attempts int
	WaitBase time.Duration
	Backoff  bool
}

type Option func(config *RetryConfig)

func (c *RetryConfig) Defaults() {
	c.Attempts = DefaultAttempts
	c.WaitBase = DefaultWaitBase
	c.Backoff = DefaultBackoff
}

func WithExponentialBackoff(attempts int, waitBase time.Duration) Option {
	return func(config *RetryConfig) {
		config.Backoff = true
		config.Attempts = attempts
		config.WaitBase = waitBase
	}
}

func WithConstant(attempts int, waitBase time.Duration) Option {
	return func(config *RetryConfig) {
		config.Attempts = attempts
		config.WaitBase = waitBase
	}
}
