package zaws

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/stretchr/testify/assert"
)

func Test_createQueueTags(t *testing.T) {
	t.Run("createQueueTags returns a formatted map of string pointers when given a map of strings", func(t *testing.T) {
		testMap := map[string]string{
			"env":          "test",
			"serviceName":  "test-service",
			"serviceGroup": "test-service-group",
			"businessUnit": "test-business-unit",
			"ownerEmail":   "owner-email",
		}

		got := createQueueTags(testMap)

		assert.Equal(t, testMap, got)
	})
}

func Test_createTopicTags(t *testing.T) {
	t.Run("createTopicTags returns a slice of aws.Tag when given a map of strings", func(t *testing.T) {
		testMap := map[string]string{
			"env":          "test",
			"serviceName":  "test-service",
			"serviceGroup": "test-service-group",
			"businessUnit": "test-business-unit",
			"ownerEmail":   "owner-email",
		}

		tag1 := types.Tag{
			Key:   aws.String("businessUnit"),
			Value: aws.String("test-business-unit"),
		}

		tag2 := types.Tag{
			Key:   aws.String("env"),
			Value: aws.String("test"),
		}

		tag3 := types.Tag{
			Key:   aws.String("ownerEmail"),
			Value: aws.String("owner-email"),
		}

		tag4 := types.Tag{
			Key:   aws.String("serviceGroup"),
			Value: aws.String("test-service-group"),
		}

		tag5 := types.Tag{
			Key:   aws.String("serviceName"),
			Value: aws.String("test-service"),
		}

		want := []types.Tag{tag1, tag2, tag3, tag4, tag5}

		got := createTopicTags(testMap)

		assert.Equal(t, want, got)
	})
}

func Test_validateTags(t *testing.T) {
	type args struct {
		tags map[string]string
	}

	goodTestMap := map[string]string{
		"env":          "test",
		"serviceName":  "test-service",
		"serviceGroup": "test-service-group",
		"businessUnit": "test-business-unit",
		"ownerEmail":   "owner-email",
	}

	badTestMap := map[string]string{
		"env":          "test",
		"businessUnit": "test-business-unit",
		"supportId":    "support-id",
	}

	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "tag validation should pass",
			args: args{
				goodTestMap,
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				assert.NoError(t, err)
				return true
			},
		},
		{
			name: "tag validation should fail",
			args: args{
				badTestMap,
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				assert.Error(t, err)
				return false
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.wantErr(t, validateTags(tt.args.tags), fmt.Sprintf("validateTags(%v)", tt.args.tags))
		})
	}
}
