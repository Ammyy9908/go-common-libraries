package zaws

import (
	"ammyy9908/go-common-libraries/messaging/mock"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/goccy/go-json"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func returnAwsQueueAttributesQueue() map[string]string {
	return map[string]string{
		"ReceiveMessageWaitTimeSeconds": "20",
		"DelaySeconds":                  "0",
		"MessageRetentionPeriod":        "10",
		"VisibilityTimeout":             "10",
	}
}

func returnExpectedQueueConfigQueue(tags map[string]string) QueueConfig {
	return QueueConfig{
		Tags:                      tags,
		ReceiveWaitTimeSeconds:    20,
		MaxReceiveCount:           10,
		DelaySeconds:              0,
		MaxMessageRetentionPeriod: 10,
		DefaultVisibilityTimeout:  10,
	}
}

func Test_validateQueueName(t *testing.T) {
	t.Run("validateQueueName returns no errors when a valid queue name is given", func(t *testing.T) {
		tests := []struct {
			queueName string
		}{
			{"test-queue"},
			{"test-queue-1"},
			{"test_queue"},
			{"TEST-QUEUE"},
			{"TEST_QUEUE_ERROR"},
			{"test-queue_ERROR"},
			{strings.Repeat("q", 80)},
		}
		for _, tt := range tests {
			name := fmt.Sprintf("%s : is valid", tt.queueName)
			t.Run(name, func(t *testing.T) {
				got := validateQueueName(tt.queueName)
				assert.Nil(t, got)
			})
		}
	})

	t.Run("validateQueueName returns an error when queueName is invalid", func(t *testing.T) {
		tests := []struct {
			queueName string
		}{
			{"test queue"},
			{"test~queue"},
			{"test-queue 1"},
			{"@Test-Queue"},
			{"test_QUEUE#3"},
			{""},
			{strings.Repeat("q", 81)},
		}
		for _, tt := range tests {
			name := fmt.Sprintf("%s : is invalid", tt.queueName)
			t.Run(name, func(t *testing.T) {
				got := validateQueueName(tt.queueName)
				assert.NotNil(t, got)
			})
		}
	})
}

func Test_getQueueArn(t *testing.T) {
	ctx := context.Background()
	t.Run("getQueueArn returns an ARN when given a queue URL", func(t *testing.T) {
		sqsClient := mock.NewMockISQSClient(gomock.NewController(t))
		queueURL := "https://sqs.ap-south-1.amazonaws.com/000000000000000/test-queue"

		sqsClient.
			EXPECT().
			GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
				AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
				QueueUrl:       aws.String(queueURL),
			}).
			Return(&sqs.GetQueueAttributesOutput{Attributes: map[string]string{"QueueArn": "arn:aws:sqs:ap-south-1:000000000000000:test-queue"}}, nil)

		want := "arn:aws:sqs:ap-south-1:000000000000000:test-queue"
		got, err := getQueueArn(sqsClient, queueURL)

		assert.Equal(t, want, got)
		assert.Nil(t, err)
	})
}

func TestGetQueueUrl(t *testing.T) {
	ctx := context.Background()
	t.Run("getQueueURL returns queue url when given the queue name", func(t *testing.T) {
		queueName := "test-queue"
		sqsClient := mock.NewMockISQSClient(gomock.NewController(t))
		sqsClient.
			EXPECT().
			GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
				QueueName: aws.String(queueName),
			}).
			Return(&sqs.GetQueueUrlOutput{QueueUrl: aws.String("www." + queueName + ".com")}, nil)

		want := "www.test-queue.com"
		got, err := GetQueueURL(sqsClient, queueName)

		assert.Equal(t, want, got)
		assert.Nil(t, err)
	})

	t.Run("getQueueURL returns error when the queue doesn't exist", func(t *testing.T) {
		queueName := "test-queue"
		sqsClient := mock.NewMockISQSClient(gomock.NewController(t))
		sqsClient.
			EXPECT().
			GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
				QueueName: aws.String(queueName),
			}).
			Return(&sqs.GetQueueUrlOutput{QueueUrl: aws.String("")}, &types.QueueDoesNotExist{})

		_, err := GetQueueURL(sqsClient, queueName)

		assert.NotNil(t, err)
	})
}

func Test_createQueue(t *testing.T) {
	ctx := context.Background()
	tags := map[string]string{
		"serviceName":  "test-service",
		"businessUnit": "test-BU",
		"environment":  "test",
		"env":          "test",
		"serviceGroup": "test-service-group",
		"ownerEmail":   "owner-email",
	}
	queueName := "test-queue"
	sqsClient := mock.NewMockISQSClient(gomock.NewController(t))

	t.Run("createQueue creates a queue when given a a name and tags", func(t *testing.T) {
		sqsClient.
			EXPECT().
			CreateQueue(ctx,
				&sqs.CreateQueueInput{
					Attributes: returnAwsQueueAttributesQueue(),
					QueueName:  aws.String(queueName),
					Tags: map[string]string{
						"serviceName":  "test-service",
						"businessUnit": "test-BU",
						"environment":  "test",
						"env":          "test",
						"serviceGroup": "test-service-group",
						"ownerEmail":   "owner-email",
					},
				}).
			Return(&sqs.CreateQueueOutput{QueueUrl: aws.String("https://sqs.ap-south-1.amazonaws.com/000000000000000/test-queue")}, nil)
		c := returnExpectedQueueConfigQueue(tags)

		want := &sqs.CreateQueueOutput{QueueUrl: aws.String("https://sqs.ap-south-1.amazonaws.com/000000000000000/test-queue")}
		got, err := createQueue(sqsClient, queueName, c)

		assert.Equal(t, want, got)
		assert.Nil(t, err)
	})

	t.Run("createQueue returns an error when aws service returns returns an error", func(t *testing.T) {
		sqsClient.
			EXPECT().
			CreateQueue(ctx,
				&sqs.CreateQueueInput{
					Attributes: returnAwsQueueAttributesQueue(),
					QueueName:  aws.String(queueName),
					Tags: map[string]string{
						"serviceName":  "test-service",
						"businessUnit": "test-BU",
						"environment":  "test",
						"env":          "test",
						"serviceGroup": "test-service-group",
						"ownerEmail":   "owner-email",
					},
				}).
			Return(&sqs.CreateQueueOutput{QueueUrl: aws.String("https://sqs.ap-south-1.amazonaws.com/000000000000000/test-queue")}, errors.New("error"))

		c := returnExpectedQueueConfigQueue(tags)

		_, err := createQueue(sqsClient, queueName, c)

		assert.NotNil(t, err)

	})
}

func Test_setUpDlq(t *testing.T) {
	ctx := context.Background()
	sqsClient := mock.NewMockISQSClient(gomock.NewController(t))

	t.Run("setUpDeadLetterQueue sets up queue attributes for dead-letter-queue when queue is created", func(t *testing.T) {
		queueURL := "https://sqs.ap-south-1.amazonaws.com/000000000000000/test-queue"
		dqlURL := "https://sqs.ap-south-1.amazonaws.com/000000000000000/test-queue_ERROR"

		sqsClient.
			EXPECT().
			GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
				AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
				QueueUrl:       aws.String(dqlURL),
			}).
			Return(&sqs.GetQueueAttributesOutput{
				Attributes: map[string]string{"QueueArn": "arn:aws:sqs:ap-south-1:000000000000000:test-queue_ERROR"}},
				nil)

		policy := map[string]string{
			"deadLetterTargetArn": "arn:aws:sqs:ap-south-1:000000000000000:test-queue_ERROR",
			"maxReceiveCount":     strconv.Itoa(10),
		}

		b, _ := json.Marshal(policy)

		sqsClient.
			EXPECT().
			SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{
				QueueUrl: aws.String(queueURL),
				Attributes: map[string]string{
					string(types.QueueAttributeNameRedrivePolicy): string(b),
				},
			}).
			Return(&sqs.SetQueueAttributesOutput{}, nil)

		maxReceiveCount := 10

		err := setUpDeadLetterQueue(sqsClient, queueURL, dqlURL, maxReceiveCount)

		assert.Nil(t, err)
	})
}

func Test_setupNewQueue(t *testing.T) {
	ctx := context.Background()
	queueName := "test-queue"
	tags := map[string]string{
		"serviceName":  "test-service",
		"businessUnit": "test-BU",
		"environment":  "test",
		"env":          "test",
		"serviceGroup": "test-service-group",
		"ownerEmail":   "owner-email",
	}
	dlqURL := "https://sqs.ap-south-1.amazonaws.com/000000000000000/test-queue_ERROR"
	dlqARN := "arn:aws:sqs:ap-south-1:000000000000000:test-queue_ERROR"
	sqsClient := mock.NewMockISQSClient(gomock.NewController(t))

	sqsClient.
		EXPECT().
		CreateQueue(ctx, &sqs.CreateQueueInput{
			Attributes: returnAwsQueueAttributesQueue(),
			QueueName:  aws.String(queueName),
			Tags: map[string]string{
				"serviceName":  "test-service",
				"businessUnit": "test-BU",
				"environment":  "test",
				"env":          "test",
				"serviceGroup": "test-service-group",
				"ownerEmail":   "owner-email",
			},
		}).
		Return(&sqs.CreateQueueOutput{QueueUrl: aws.String("https://sqs.ap-south-1.amazonaws.com/000000000000000/test-queue")}, nil)

	sqsClient.
		EXPECT().
		CreateQueue(ctx, &sqs.CreateQueueInput{
			Attributes: returnAwsQueueAttributesQueue(),
			QueueName:  aws.String(queueName + ErrorQueueSuffix),
			Tags: map[string]string{
				"serviceName":  "test-service",
				"businessUnit": "test-BU",
				"environment":  "test",
				"env":          "test",
				"serviceGroup": "test-service-group",
				"ownerEmail":   "owner-email",
			},
		}).
		Return(&sqs.CreateQueueOutput{QueueUrl: aws.String(dlqURL)}, nil)

	sqsClient.
		EXPECT().
		GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
			AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
			QueueUrl:       aws.String(dlqURL),
		}).
		Return(&sqs.GetQueueAttributesOutput{
			Attributes: map[string]string{"QueueArn": dlqARN}},
			nil)

	policy := map[string]string{
		"deadLetterTargetArn": dlqARN,
		"maxReceiveCount":     strconv.Itoa(10),
	}

	b, _ := json.Marshal(policy)

	sqsClient.
		EXPECT().
		SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{
			QueueUrl: aws.String("https://sqs.ap-south-1.amazonaws.com/000000000000000/test-queue"),
			Attributes: map[string]string{
				string(types.QueueAttributeNameRedrivePolicy): string(b),
			},
		}).
		Return(&sqs.SetQueueAttributesOutput{}, nil)

	t.Run("setUpNewQueue returns Queue creation output when given queue name and tags", func(t *testing.T) {
		c := returnExpectedQueueConfigQueue(tags)

		want := &sqs.CreateQueueOutput{QueueUrl: aws.String("https://sqs.ap-south-1.amazonaws.com/000000000000000/test-queue")}
		got, err := setupNewQueue(sqsClient, queueName, c)

		assert.Equal(t, want, got)
		assert.Nil(t, err)
	})
}
