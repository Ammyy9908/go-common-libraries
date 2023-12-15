package zaws

import (
	"errors"
	"math"
	"math/rand"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sns/types"
)

type publishFunc func(message string) error
type publishWithAttributesFunc func(message string, attributes map[string]string) error

func Retry(publishFunc publishFunc, message string, opts ...Option) error {
	fn := func() error {
		return publishFunc(message)
	}
	return defaultRetry(fn, opts...)
}

func RetryWithAttributes(publishFunc publishWithAttributesFunc, message string, attributes map[string]string, opts ...Option) error {
	fn := func() error {
		return publishFunc(message, attributes)
	}
	return defaultRetry(fn, opts...)
}

func defaultRetry(function func() error, opts ...Option) error {
	var config RetryConfig
	var err error
	var i int
	config.Defaults()

	for _, option := range opts {
		option(&config)
	}

	for i = 0; i <= config.Attempts; i++ {
		err = function()
		var throttledErr *types.ThrottledException
		if err == nil || !errors.As(err, &throttledErr) {
			break
		}
		waitDuration := config.WaitBase
		if config.Backoff {
			exp := math.Exp2(float64(i + 1))
			waitDuration = time.Duration(float64(config.WaitBase) * exp)
			waitDuration = waitDuration.Round(time.Millisecond)
			random := rand.New(rand.NewSource(time.Now().UnixNano()))
			waitDuration = time.Duration(float64(waitDuration) * random.Float64())
		}

		time.Sleep(waitDuration)
	}
	return err
}
