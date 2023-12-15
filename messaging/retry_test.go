package zaws

import (
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/stretchr/testify/assert"
)

var errReturn = errors.New("wanted error")
var errThrottle = &types.ThrottledException{}

func Test_retry(t *testing.T) {
	type args struct {
		timesThrottled int
		opt            testOption
		attempts       int
		waitBase       time.Duration
		wantError      error
	}
	tests := []struct {
		name string
		args args
	}{
		{name: "Retry With Constant options throttles less times than maximum attempts hence returns the last non-throttle error", args: args{
			timesThrottled: 2,
			opt:            WithConstant,
			attempts:       3,
			waitBase:       1 * time.Millisecond,
			wantError:      errReturn,
		}},
		{name: "Retry With Constant options throttles more times than maximum attempts hence returns throttle error", args: args{
			timesThrottled: 5,
			opt:            WithConstant,
			attempts:       3,
			waitBase:       1 * time.Millisecond,
			wantError:      errThrottle,
		}},
		{name: "Retry With Constant options throttles only the first time and returns first non-throttle error", args: args{
			timesThrottled: 1,
			opt:            WithConstant,
			attempts:       1,
			waitBase:       1 * time.Millisecond,
			wantError:      errReturn,
		}},
		{name: "Retry With Exponential options throttles less times than maximum attempts hence returns the last non-throttle error", args: args{
			timesThrottled: 2,
			opt:            WithExponentialBackoff,
			attempts:       3,
			waitBase:       1 * time.Millisecond,
			wantError:      errReturn,
		}},
		{name: "Retry With Exponential options throttles more times than maximum attempts hence returns throttle error", args: args{
			timesThrottled: 5,
			opt:            WithExponentialBackoff,
			attempts:       3,
			waitBase:       1 * time.Millisecond,
			wantError:      errThrottle,
		}},
		{name: "Retry With Constant options throttles only the first time and returns first non-throttle error", args: args{
			timesThrottled: 1,
			opt:            WithExponentialBackoff,
			attempts:       1,
			waitBase:       1 * time.Millisecond,
			wantError:      errReturn,
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rc := retryCounter{
				runs:           0,
				timesThrottled: tt.args.timesThrottled,
			}
			got := Retry(rc.RetryPublish, "test-message", tt.args.opt(tt.args.attempts, tt.args.waitBase))

			assert.Equal(t, got, tt.args.wantError)
			// +1 for initial attempt
			assert.LessOrEqual(t, rc.runs, tt.args.attempts+1)
		})
	}
}

type testOption func(attempts int, waitBase time.Duration) Option

type retryCounter struct {
	runs           int
	timesThrottled int
}

func (r *retryCounter) RetryPublish(message string) error {
	r.runs++
	if r.runs > r.timesThrottled {
		return errReturn
	}
	return errThrottle
}
