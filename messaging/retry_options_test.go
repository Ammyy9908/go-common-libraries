package zaws

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestWithExponentialBackoff(t *testing.T) {
	tests := inputSetup()
	for _, tt := range tests {
		t.Run(fmt.Sprintf("ExponentialBackoff with attempts %d and wait time %v", tt.args.attempts, tt.args.waitBase), func(t *testing.T) {
			c := RetryConfig{}
			c.Defaults()
			opt := WithExponentialBackoff(tt.args.attempts, tt.args.waitBase)
			opt(&c)

			assert.Equal(t, tt.args.attempts, c.Attempts)
			assert.Equal(t, tt.args.waitBase, c.WaitBase)
			assert.True(t, c.Backoff)
		})
	}
}

func TestWithConstant(t *testing.T) {
	tests := inputSetup()
	for _, tt := range tests {
		t.Run(fmt.Sprintf("WithConstant with attempts %d and wait time %v", tt.args.attempts, tt.args.waitBase), func(t *testing.T) {
			c := RetryConfig{}
			c.Defaults()
			opt := WithConstant(tt.args.attempts, tt.args.waitBase)
			opt(&c)

			assert.Equal(t, tt.args.attempts, c.Attempts)
			assert.Equal(t, tt.args.waitBase, c.WaitBase)
			assert.False(t, c.Backoff)
		})
	}
}

type args struct {
	attempts int
	waitBase time.Duration
}

func inputSetup() []struct{ args args } {
	tests := []struct {
		args args
	}{
		{args: args{
			attempts: 2,
			waitBase: 3 * time.Second,
		}},
		{args: args{
			attempts: 5,
			waitBase: 5 * time.Millisecond,
		}},
		{args: args{
			attempts: 3,
			waitBase: 10 * time.Minute,
		}},
	}

	return tests
}
