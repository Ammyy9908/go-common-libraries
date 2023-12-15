package zaws

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/ammyy9908/go-common-libraries/messaging/mock"

	"github.com/aws/smithy-go/middleware"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/goccy/go-json"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func returnAwsQueueAttributesManager() map[string]string {
	return map[string]string{
		"ReceiveMessageWaitTimeSeconds": "20",
		"DelaySeconds":                  "0",
		"MessageRetentionPeriod":        "10",
		"VisibilityTimeout":             "10",
	}
}

func returnExpectedQueueConfigManager(tags map[string]string) QueueConfig {
	return QueueConfig{
		Tags:                      tags,
		ReceiveWaitTimeSeconds:    20,
		MaxReceiveCount:           10,
		DelaySeconds:              0,
		MaxMessageRetentionPeriod: 10,
		DefaultVisibilityTimeout:  10,
	}
}

func Test_validateReceiveTimeWaitSeconds(t *testing.T) {
	t.Run("validateReceiveTimeWaitSeconds does not return an error when given a valid time limit", func(t *testing.T) {
		waitTime := 10
		err := validateReceiveTimeWaitSeconds(waitTime)

		assert.Nil(t, err)
	})

	t.Run("validateReceiveTimeWaitSeconds returns an error when the input exceeds limit", func(t *testing.T) {
		waitTime := 25
		err := validateReceiveTimeWaitSeconds(waitTime)

		assert.Equal(t, errors.New(errWaitTimeMoreThan20), err)
	})

	t.Run("validateReceiveTimeWaitSeconds returns an error when the input is non-positive", func(t *testing.T) {
		waitTime := -10
		err := validateReceiveTimeWaitSeconds(waitTime)

		assert.Equal(t, errors.New(errWaitTimeNegative), err)
	})
}

func TestManager_CreateQueue(t *testing.T) {
	queueName := "test-queue"
	tags := map[string]string{
		"env":          "test",
		"serviceName":  "test-service",
		"serviceGroup": "test-service-group",
		"businessUnit": "test-business-unit",
		"ownerEmail":   "owner-email",
	}

	ctrl := gomock.NewController(t)
	snsClient := mock.NewMockISNSClient(ctrl)
	sqsClient := mock.NewMockISQSClient(ctrl)
	ctx := context.Background()

	t.Run("CreateQueue creates a queue when given a name and tags", func(t *testing.T) {
		sqsClient.
			EXPECT().
			CreateQueue(ctx, &sqs.CreateQueueInput{
				Attributes: returnAwsQueueAttributesManager(),
				QueueName:  aws.String(queueName),
				Tags:       createQueueTags(tags),
			}).
			Return(&sqs.CreateQueueOutput{
				QueueUrl: aws.String("https://sqs.ap-south-1.amazonaws.com/000000000000000/test-queue"),
			}, nil)

		sqsClient.
			EXPECT().
			CreateQueue(ctx, &sqs.CreateQueueInput{
				QueueName:  aws.String(queueName + ErrorQueueSuffix),
				Tags:       createQueueTags(tags),
				Attributes: returnAwsQueueAttributesManager(),
			}).
			Return(&sqs.CreateQueueOutput{
				QueueUrl: aws.String("https://sqs.ap-south-1.amazonaws.com/000000000000000/test-queue_ERROR"),
			}, nil)

		reDrivePolicy := map[string]string{
			"deadLetterTargetArn": "arn:aws:sqs:ap-south-1:000000000000000:test-queue_ERROR",
			"maxReceiveCount":     "10",
		}

		p, _ := json.Marshal(reDrivePolicy)

		sqsClient.
			EXPECT().
			GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
				AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
				QueueUrl:       aws.String("https://sqs.ap-south-1.amazonaws.com/000000000000000/test-queue_ERROR"),
			}).
			Return(&sqs.GetQueueAttributesOutput{Attributes: map[string]string{
				string(types.QueueAttributeNameRedrivePolicy): string(p),
				string(types.QueueAttributeNameQueueArn):      "arn:aws:sqs:ap-south-1:000000000000000:test-queue_ERROR",
			}}, nil)

		sqsClient.
			EXPECT().
			SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{
				QueueUrl: aws.String("https://sqs.ap-south-1.amazonaws.com/000000000000000/test-queue"),
				Attributes: map[string]string{
					string(types.QueueAttributeNameRedrivePolicy): string(p),
				},
			}).
			Return(&sqs.SetQueueAttributesOutput{},
				nil)

		m := &Manager{
			snsClient: snsClient,
			sqsClient: sqsClient,
		}

		c := returnExpectedQueueConfigManager(tags)

		want := &sqs.CreateQueueOutput{QueueUrl: aws.String("https://sqs.ap-south-1.amazonaws.com/000000000000000/test-queue")}

		got, err := m.CreateQueue(queueName, c)

		assert.Equal(t, want, got)
		assert.Nil(t, err)
	})

	t.Run("CreateQueue returns an errQueueNameTooLong when given too long queueName", func(t *testing.T) {
		m := &Manager{
			snsClient: snsClient,
			sqsClient: sqsClient,
		}

		c := QueueConfig{
			Tags:                   tags,
			ReceiveWaitTimeSeconds: 20,
			MaxReceiveCount:        10,
		}

		var b bytes.Buffer
		for i := 0; i < 100; i++ {
			b.WriteString("x")
		}

		got, err := m.CreateQueue(b.String(), c)

		assert.Nil(t, got)
		assert.Equal(t, errors.New(errQueueNameTooLong), err)
	})

	t.Run("CreateQueue returns an errQueueNameEmpty when given empty queueName", func(t *testing.T) {
		m := &Manager{
			snsClient: snsClient,
			sqsClient: sqsClient,
		}

		c := QueueConfig{
			Tags:                   tags,
			ReceiveWaitTimeSeconds: 20,
			MaxReceiveCount:        10,
		}

		got, err := m.CreateQueue("", c)

		assert.Nil(t, got)
		assert.Equal(t, errors.New(errQueueNameEmpty), err)
	})

	t.Run("CreateQueue returns an errNonAlphaNumericCharsInQueueName when given queueName containing non-alphanumeric characters", func(t *testing.T) {
		m := &Manager{
			snsClient: snsClient,
			sqsClient: sqsClient,
		}

		c := QueueConfig{
			Tags:                   tags,
			ReceiveWaitTimeSeconds: 20,
			MaxReceiveCount:        10,
		}

		got, err := m.CreateQueue("queue $$$", c)

		assert.Nil(t, got)
		assert.Equal(t, errors.New(errNonAlphaNumericCharsInQueueName), err)
	})

	t.Run("CreateQueue returns an ErrMissingTags when there are no tags", func(t *testing.T) {
		m := &Manager{
			snsClient: snsClient,
			sqsClient: sqsClient,
		}
		var badTags map[string]string

		c := QueueConfig{
			Tags:                   badTags,
			ReceiveWaitTimeSeconds: 20,
			MaxReceiveCount:        10,
		}

		got, err := m.CreateQueue("test-queue", c)

		assert.Nil(t, got)
		assert.Equal(t, errors.New(ErrMissingTags), err)
	})

	t.Run("CreateQueue returns errWaitTimeMoreThan20 when given invalid receiveWaitTime", func(t *testing.T) {
		m := &Manager{
			snsClient: snsClient,
			sqsClient: sqsClient,
		}

		c := QueueConfig{
			Tags:                   tags,
			ReceiveWaitTimeSeconds: 30,
			MaxReceiveCount:        10,
		}

		got, err := m.CreateQueue("test-queue", c)

		assert.Nil(t, got)
		assert.Equal(t, errors.New(errWaitTimeMoreThan20), err)
	})

	t.Run("CreateQueue returns an error when the queue was not created", func(t *testing.T) {
		errorMessage := "internal error"
		awsError := &types.InvalidIdFormat{Message: &errorMessage}
		sqsClient.
			EXPECT().
			CreateQueue(ctx, &sqs.CreateQueueInput{
				Attributes: returnAwsQueueAttributesManager(),
				QueueName:  aws.String(queueName),
				Tags:       createQueueTags(tags),
			}).
			Return(&sqs.CreateQueueOutput{}, awsError)

		m := &Manager{
			snsClient: snsClient,
			sqsClient: sqsClient,
		}

		c := returnExpectedQueueConfigManager(tags)

		got, err := m.CreateQueue(queueName, c)

		assert.Nil(t, got)
		assert.Equal(t, awsError, err)
	})

	t.Run("CreateQueue returns an error when the deadletter queue was not created", func(t *testing.T) {
		errorMessage := "internal error"
		awsError := &types.InvalidIdFormat{Message: &errorMessage}
		sqsClient.
			EXPECT().
			CreateQueue(ctx, &sqs.CreateQueueInput{
				Attributes: returnAwsQueueAttributesManager(),
				QueueName:  aws.String(queueName),
				Tags:       createQueueTags(tags),
			}).
			Return(&sqs.CreateQueueOutput{
				QueueUrl: aws.String("https://sqs.ap-south-1.amazonaws.com/000000000000000/test-queue"),
			}, nil).
			Times(1)

		sqsClient.
			EXPECT().
			CreateQueue(ctx, &sqs.CreateQueueInput{
				Attributes: returnAwsQueueAttributesManager(),
				QueueName:  aws.String(queueName + ErrorQueueSuffix),
				Tags:       createQueueTags(tags),
			}).
			Return(&sqs.CreateQueueOutput{}, awsError).
			Times(1)

		m := &Manager{
			snsClient: snsClient,
			sqsClient: sqsClient,
		}

		c := returnExpectedQueueConfigManager(tags)

		got, err := m.CreateQueue(queueName, c)

		assert.Nil(t, got)
		assert.Equal(t, awsError, err)
	})
}

func TestManager_CreateTopic(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	snsClient := mock.NewMockISNSClient(ctrl)
	sqsClient := mock.NewMockISQSClient(ctrl)

	topicName := "test-topic"

	tags := map[string]string{
		"businessUnit": "test-business-unit",
		"ownerEmail":   "owner-email",
		"env":          "test",
		"serviceName":  "test-service",
		"serviceGroup": "test-service-group",
	}

	t.Run("CreateTopic creates a topic when given a topic name", func(t *testing.T) {
		createdTags := createTopicTags(tags)
		snsClient.
			EXPECT().
			CreateTopic(ctx, &sns.CreateTopicInput{
				Name: aws.String(topicName),
				Tags: createdTags,
			}).
			Return(&sns.CreateTopicOutput{TopicArn: aws.String("arn:test-topic")}, nil)

		m := &Manager{
			snsClient: snsClient,
			sqsClient: sqsClient,
		}

		want := &sns.CreateTopicOutput{TopicArn: aws.String("arn:test-topic")}
		got, err := m.CreateTopic("test-topic", tags)

		assert.Equal(t, want, got)
		assert.Nil(t, err)

	})

	t.Run("Create topic returns an error when topic creation resulted in an error", func(t *testing.T) {
		snsClient.
			EXPECT().
			CreateTopic(ctx, &sns.CreateTopicInput{
				Name: aws.String(topicName),
				Tags: createTopicTags(tags),
			}).
			Return(&sns.CreateTopicOutput{}, errors.New("topic creation error"))

		m := &Manager{
			snsClient: snsClient,
			sqsClient: sqsClient,
		}

		got, err := m.CreateTopic("test-topic", tags)

		assert.Nil(t, got)
		assert.NotNil(t, err)
	})

	t.Run("CreateTopic returns ErrMissingTags when the tags are empty", func(t *testing.T) {
		m := &Manager{
			snsClient: snsClient,
			sqsClient: sqsClient,
		}

		var badTags map[string]string

		got, err := m.CreateTopic("test-topic", badTags)

		assert.Nil(t, got)
		assert.Equal(t, errors.New(ErrMissingTags), err)
	})
}

func TestManager_subscribe(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	snsClient := mock.NewMockISNSClient(ctrl)
	sqsClient := mock.NewMockISQSClient(ctrl)
	raw := true

	t.Run("Subscribe subscribes a queue to a topic and does not return error when given queue name and topic name", func(t *testing.T) {
		snsClient.
			EXPECT().
			Subscribe(ctx, &sns.SubscribeInput{
				TopicArn: aws.String("test-topic"),
				Protocol: aws.String("sqs"),
				Endpoint: aws.String("test-queue"),
				Attributes: map[string]string{
					"RawMessageDelivery": "true",
				},
			}).
			Return(&sns.SubscribeOutput{SubscriptionArn: aws.String("arn:test-topic")}, nil)

		m := Manager{
			snsClient: snsClient,
			sqsClient: sqsClient,
		}

		got, err := m.subscribe("test-topic", "test-queue", raw)

		assert.NotEmpty(t, got)
		assert.Nil(t, err)
	})

	t.Run("Subscribe returns an error when the subscription fails", func(t *testing.T) {
		subscriptionError := errors.New("subscription error")
		snsClient.
			EXPECT().
			Subscribe(ctx, &sns.SubscribeInput{
				TopicArn: aws.String("test-topic"),
				Protocol: aws.String("sqs"),
				Endpoint: aws.String("test-queue"),
				Attributes: map[string]string{
					"RawMessageDelivery": "true",
				},
			}).
			Return(&sns.SubscribeOutput{SubscriptionArn: aws.String("arn:test-topic")}, subscriptionError)

		m := Manager{
			snsClient: snsClient,
			sqsClient: sqsClient,
		}

		got, err := m.subscribe("test-topic", "test-queue", raw)

		assert.Empty(t, got)
		assert.Equal(t, subscriptionError, err)
	})
}

func TestManager_SubscribeQueueToTopic(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	snsClient := mock.NewMockISNSClient(ctrl)
	sqsClient := mock.NewMockISQSClient(ctrl)
	topicName := "test-topic-name"
	topicArn := "arn:aws:sns:ap-south-1:00000000:test-topic"
	queueArn := "test-queue-arn"
	queueName := "test-queue-name"
	queueURL := "test-queue-url"
	subscriptionArn := "arn:aws:sns:ap-south-1:00000000:test-topic"
	awsError := errors.New("Service internal error")
	attributes := make(map[string]string, 1)
	raw := true

	t.Run("SubscribeQueueToTopic subscribes a queue to a topic and does not return error when given queue name and topic name", func(t *testing.T) {
		snsClient.EXPECT().
			CreateTopic(ctx, &sns.CreateTopicInput{
				Name: aws.String(topicName),
			}).
			Return(&sns.CreateTopicOutput{TopicArn: aws.String(topicArn)}, nil)

		sqsClient.EXPECT().
			GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
				QueueName: aws.String(queueName),
			}).
			Return(&sqs.GetQueueUrlOutput{QueueUrl: aws.String(queueURL)}, nil)

		sqsClient.EXPECT().
			GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
				AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
				QueueUrl:       aws.String(queueURL),
			}).
			Return(&sqs.GetQueueAttributesOutput{
				Attributes: map[string]string{
					string(types.QueueAttributeNameQueueArn): queueArn,
				},
			}, nil)

		snsClient.EXPECT().
			Subscribe(ctx, &sns.SubscribeInput{
				TopicArn: aws.String(topicArn),
				Protocol: aws.String("sqs"),
				Endpoint: aws.String(queueArn),
				Attributes: map[string]string{
					"RawMessageDelivery": "true",
				},
			}).
			Return(&sns.SubscribeOutput{SubscriptionArn: aws.String(topicArn)}, nil)

		sqsClient.EXPECT().
			GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
				AttributeNames: []types.QueueAttributeName{types.QueueAttributeNamePolicy},
				QueueUrl:       aws.String(queueURL),
			}).
			Return(&sqs.GetQueueAttributesOutput{
				Attributes:     map[string]string{},
				ResultMetadata: middleware.Metadata{},
			}, nil).
			AnyTimes()

		attributes["Policy"] = "{\"Version\":\"2012-10-17\",\"Id\":\"arn:test-queue/SQSDefaultPolicy\",\"Statement\":[{\"Sid\":\"Sid1580665629194\",\"Effect\":\"Allow\",\"Principal\":{\"Service\":\"*\"},\"Action\":\"SQS:SendMessage\",\"Resource\":\"test-queue-url\",\"Condition\":{\"ArnEquals\":{\"aws:SourceArn\":\"arn:aws:sns:ap-south-1:00000000:test-topic\"}}}]}"

		sqsClient.EXPECT().
			SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{
				QueueUrl:   aws.String(queueURL),
				Attributes: attributes,
			}).Return(&sqs.SetQueueAttributesOutput{}, nil)

		m := Manager{
			snsClient: snsClient,
			sqsClient: sqsClient,
		}

		err := m.SubscribeQueueToTopic(queueName, topicName, raw)
		assert.Nil(t, err)
	})

	t.Run("SubscribeQueueToTopic returns error when CreateTopic returns error", func(t *testing.T) {
		snsClient.EXPECT().
			CreateTopic(ctx, &sns.CreateTopicInput{
				Name: aws.String(topicName),
			}).
			Return(nil, awsError)

		m := Manager{
			snsClient: snsClient,
			sqsClient: sqsClient,
		}

		err := m.SubscribeQueueToTopic(queueName, topicName, raw)
		assert.Equal(t, awsError, err)
	})

	t.Run("SubscribeQueueToTopic returns error when GetQueueUrl returns error", func(t *testing.T) {
		snsClient.EXPECT().
			CreateTopic(ctx, &sns.CreateTopicInput{
				Name: aws.String(topicName),
			}).
			Return(&sns.CreateTopicOutput{TopicArn: aws.String(topicArn)}, nil)

		sqsClient.EXPECT().
			GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
				QueueName: aws.String(queueName),
			}).
			Return(nil, awsError)

		m := Manager{
			snsClient: snsClient,
			sqsClient: sqsClient,
		}

		err := m.SubscribeQueueToTopic(queueName, topicName, raw)
		assert.Equal(t, awsError, err)
	})

	t.Run("SubscribeQueueToTopic returns error when GetQueueAttributes returns error", func(t *testing.T) {
		snsClient.EXPECT().
			CreateTopic(ctx, &sns.CreateTopicInput{
				Name: aws.String(topicName),
			}).
			Return(&sns.CreateTopicOutput{TopicArn: aws.String(topicArn)}, nil)

		sqsClient.EXPECT().
			GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
				QueueName: aws.String(queueName),
			}).
			Return(&sqs.GetQueueUrlOutput{QueueUrl: aws.String(queueURL)}, nil)

		sqsClient.EXPECT().
			GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
				AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
				QueueUrl:       aws.String(queueURL),
			}).
			Return(nil, awsError)

		m := Manager{
			snsClient: snsClient,
			sqsClient: sqsClient,
		}

		err := m.SubscribeQueueToTopic(queueName, topicName, raw)
		assert.Equal(t, awsError, err)
	})

	t.Run("SubscribeQueueToTopic returns error when Subscribe returns error", func(t *testing.T) {
		snsClient.EXPECT().
			CreateTopic(ctx, &sns.CreateTopicInput{
				Name: aws.String(topicName),
			}).
			Return(&sns.CreateTopicOutput{TopicArn: aws.String(topicArn)}, nil)

		sqsClient.EXPECT().
			GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
				QueueName: aws.String(queueName),
			}).
			Return(&sqs.GetQueueUrlOutput{QueueUrl: aws.String(queueURL)}, nil)

		sqsClient.EXPECT().
			GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
				AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
				QueueUrl:       aws.String(queueURL),
			}).
			Return(&sqs.GetQueueAttributesOutput{
				Attributes: map[string]string{
					string(types.QueueAttributeNameQueueArn): queueArn,
				},
			}, nil)

		snsClient.EXPECT().
			Subscribe(ctx, &sns.SubscribeInput{
				TopicArn: aws.String(topicArn),
				Protocol: aws.String("sqs"),
				Endpoint: aws.String(queueArn),
				Attributes: map[string]string{
					"RawMessageDelivery": "true",
				},
			}).
			Return(nil, awsError)

		m := Manager{
			snsClient: snsClient,
			sqsClient: sqsClient,
		}

		err := m.SubscribeQueueToTopic(queueName, topicName, raw)
		assert.Equal(t, awsError, err)
	})

	t.Run("SubscribeQueueToTopic returns error when SetQueueAttributes returns error", func(t *testing.T) {
		snsClient.EXPECT().
			CreateTopic(ctx, &sns.CreateTopicInput{
				Name: aws.String(topicName),
			}).
			Return(&sns.CreateTopicOutput{TopicArn: aws.String(topicArn)}, nil)

		sqsClient.EXPECT().
			GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
				QueueName: aws.String(queueName),
			}).
			Return(&sqs.GetQueueUrlOutput{QueueUrl: aws.String(queueURL)}, nil)

		sqsClient.EXPECT().
			GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
				AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
				QueueUrl:       aws.String(queueURL),
			}).
			Return(&sqs.GetQueueAttributesOutput{
				Attributes: map[string]string{
					string(types.QueueAttributeNameQueueArn): queueArn,
				},
			}, nil)

		snsClient.EXPECT().
			Subscribe(ctx, &sns.SubscribeInput{
				TopicArn: aws.String(topicArn),
				Protocol: aws.String("sqs"),
				Endpoint: aws.String(queueArn),
				Attributes: map[string]string{
					"RawMessageDelivery": "true",
				},
			}).
			Return(&sns.SubscribeOutput{SubscriptionArn: aws.String(subscriptionArn)}, nil)

		sqsClient.EXPECT().
			SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{
				QueueUrl:   aws.String(queueURL),
				Attributes: attributes,
			}).Return(nil, awsError)

		m := Manager{
			snsClient: snsClient,
			sqsClient: sqsClient,
		}

		err := m.SubscribeQueueToTopic(queueName, topicName, raw)
		assert.Equal(t, awsError, err)
	})
}
