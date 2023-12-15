package zaws

import (
	"ammyy9908/go-common-libraries/messaging/mock"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

var (
	DummyUUID = "<some_uuid>"
)

func TestNewTopicPublisher(t *testing.T) {
	t.Run("NewTopicPublisher returns new topic publisher", func(t *testing.T) {
		publisher, err := NewTopicPublisher("ap-south-1", "test-topic")

		assert.NotEmpty(t, publisher)
		assert.Nil(t, err)
	})

	t.Run("NewTopicPublisher returns error when given a bad region", func(t *testing.T) {
		publisher, err := NewTopicPublisher("bad-region", "test-topic")

		assert.Empty(t, publisher)
		assert.NotNil(t, err)
	})
}

func TestNewTopicPublisherWithConfig(t *testing.T) {
	t.Run("NewTopicPublisherWithConfig returns error when given a bad region", func(t *testing.T) {
		testConfig := aws.NewConfig()
		testConfig.Region = "bad-environment"

		_, err := NewTopicPublisherWithConfig(*testConfig, "test-topic")

		assert.NotNil(t, err)
	})

	t.Run("NewTopicPublisherWithConfig returns error when given a bad region", func(t *testing.T) {
		testConfig := aws.NewConfig()
		testConfig.Region = "bad-environment"

		_, err := NewTopicPublisherWithConfig(*testConfig, "test-topic")

		assert.NotNil(t, err)
	})
}

func TestTopicPublisher_Publish(t *testing.T) {
	snsClient, publisher, _, topicArn := publisherTestSetup(t)
	ctx := context.Background()

	t.Run("Publish sends a message to the topic when aws publish gives no error", func(t *testing.T) {
		snsClient.
			EXPECT().
			Publish(ctx, &sns.PublishInput{
				Message:  aws.String("test message"),
				TopicArn: aws.String(topicArn),
			}).
			Return(&sns.PublishOutput{}, nil)

		err := publisher.Publish("test message")

		assert.Nil(t, err)
	})

	t.Run("Publish returns an error when the publishing causes an error", func(t *testing.T) {
		snsClient.
			EXPECT().
			Publish(ctx, &sns.PublishInput{
				Message:  aws.String("test message"),
				TopicArn: aws.String(topicArn),
			}).
			Return(&sns.PublishOutput{}, errors.New("test error"))

		err := publisher.Publish("test message")

		assert.NotNil(t, err)
	})
}

func TestTopicPublisher_PublishWithAttributes(t *testing.T) {
	snsClient, publisher, _, topicArn := publisherTestSetup(t)
	ctx := context.Background()

	t.Run("PublishWithAttributes sends a message to the topic when aws publish gives no error", func(t *testing.T) {
		snsClient.
			EXPECT().
			Publish(ctx, &sns.PublishInput{
				Message:  aws.String("test message"),
				TopicArn: aws.String(topicArn),
				MessageAttributes: map[string]types.MessageAttributeValue{
					"X-Correlation-Id": {StringValue: &DummyUUID},
				},
			}).
			Return(&sns.PublishOutput{}, nil)

		err := publisher.PublishWithAttributes("test message", map[string]string{
			"X-Correlation-Id": DummyUUID,
		})

		assert.Nil(t, err)
	})

	t.Run("PublishWithAttributes returns an error when the publishing causes an error", func(t *testing.T) {
		dummyUUID := "<some_uuid>"
		snsClient.
			EXPECT().
			Publish(ctx, &sns.PublishInput{
				Message:  aws.String("test message"),
				TopicArn: aws.String(topicArn),
				MessageAttributes: map[string]types.MessageAttributeValue{
					"X-Correlation-Id": {StringValue: &dummyUUID},
				}}).
			Return(&sns.PublishOutput{}, errors.New("test error"))

		err := publisher.PublishWithAttributes("test message", map[string]string{
			"X-Correlation-Id": dummyUUID,
		})

		assert.NotNil(t, err)
	})
}

func TestTopicPublisher_PublishEvent(t *testing.T) {
	snsClient, publisher, topicName, topicArn := publisherTestSetup(t)
	ctx := context.Background()

	t.Run("PublishEvent sends a message to the topic when aws publish gives no error", func(t *testing.T) {
		snsClient.
			EXPECT().
			Publish(ctx, &sns.PublishInput{
				Message:  aws.String("test message"),
				TopicArn: aws.String(topicArn),
				Subject:  aws.String(topicName),
			}).
			Return(&sns.PublishOutput{}, nil)

		err := publisher.PublishEvent("test message")

		assert.Nil(t, err)
	})

	t.Run("PublishEvent returns an error when the publishing causes an error", func(t *testing.T) {
		snsClient.
			EXPECT().
			Publish(ctx, &sns.PublishInput{
				Message:  aws.String("test message"),
				TopicArn: aws.String(topicArn),
				Subject:  aws.String(topicName),
			}).
			Return(&sns.PublishOutput{}, errors.New("test error"))

		err := publisher.PublishEvent("test message")

		assert.NotNil(t, err)
	})
}

func TestTopicPublisher_PublishEventWithAttributes(t *testing.T) {
	snsClient, publisher, topicName, topicArn := publisherTestSetup(t)
	ctx := context.Background()

	t.Run("PublishEventWithAttributes sends a message to the topic when aws publish gives no error", func(t *testing.T) {
		snsClient.
			EXPECT().
			Publish(ctx, &sns.PublishInput{
				Message:  aws.String("test message"),
				TopicArn: aws.String(topicArn),
				Subject:  aws.String(topicName),
				MessageAttributes: map[string]types.MessageAttributeValue{
					"X-Correlation-Id": {StringValue: &DummyUUID},
				}}).
			Return(&sns.PublishOutput{}, nil)

		err := publisher.PublishEventWithAttributes("test message", map[string]string{
			"X-Correlation-Id": DummyUUID,
		})

		assert.Nil(t, err)
	})

	t.Run("PublishEventWithAttributes returns an error when the publishing causes an error", func(t *testing.T) {
		dummyUUID := "<some_uuid>"
		snsClient.
			EXPECT().
			Publish(ctx, &sns.PublishInput{
				Message:  aws.String("test message"),
				TopicArn: aws.String(topicArn),
				Subject:  aws.String(topicName),
				MessageAttributes: map[string]types.MessageAttributeValue{
					"X-Correlation-Id": {StringValue: &dummyUUID},
				}}).
			Return(&sns.PublishOutput{}, errors.New("test error"))

		err := publisher.PublishEventWithAttributes("test message", map[string]string{
			"X-Correlation-Id": dummyUUID,
		})

		assert.NotNil(t, err)
	})
}

func TestTopicPublisher_PublishWithRetry(t *testing.T) {
	snsClient, publisher, _, _ := publisherTestSetup(t)
	want := errors.New("non throttle error")

	t.Run("PublishWithRetry runs publishing if the error was Throttled using exponential backoff", func(t *testing.T) {
		repeatThrottlingErrorSetup(t, snsClient, publisher, want, false)

		err := publisher.PublishWithRetry("test message", WithExponentialBackoff(3, 1*time.Millisecond))

		assert.NotNil(t, err)
		assert.Equal(t, want, err)
	})

	t.Run("PublishWithRetry runs publishing if the error was Throttled using constant time change", func(t *testing.T) {
		repeatThrottlingErrorSetup(t, snsClient, publisher, want, false)

		err := publisher.PublishWithRetry("test message", WithConstant(3, 1*time.Millisecond))

		assert.NotNil(t, err)
		assert.Equal(t, want, err)
	})

	t.Run("PublishWithRetry runs publishing if the error was Throttled using defaults", func(t *testing.T) {
		repeatThrottlingErrorSetup(t, snsClient, publisher, want, false)

		err := publisher.PublishWithRetry("test message")

		assert.NotNil(t, err)
		assert.Equal(t, want, err)
	})
}

func TestTopicPublisher_PublishWithAttributesWithRetry(t *testing.T) {
	snsClient, publisher, _, _ := publisherTestSetup(t)
	want := errors.New("non throttle error")
	dummyUUID := "<some_uuid>"

	t.Run("PublishWithAttributesWithRetry runs publishing if the error was Throttled using exponential backoff", func(t *testing.T) {
		repeatThrottlingErrorSetup(t, snsClient, publisher, want, true)

		err := publisher.PublishWithAttributesWithRetry("test message", map[string]string{
			"X-Correlation-Id": dummyUUID,
		}, WithExponentialBackoff(3, 1*time.Millisecond))

		assert.NotNil(t, err)
		assert.Equal(t, want, err)
	})

	t.Run("PublishWithAttributesWithRetry runs publishing if the error was Throttled using constant time change", func(t *testing.T) {
		repeatThrottlingErrorSetup(t, snsClient, publisher, want, true)

		err := publisher.PublishWithAttributesWithRetry("test message", map[string]string{
			"X-Correlation-Id": dummyUUID,
		}, WithConstant(3, 1*time.Millisecond))

		assert.NotNil(t, err)
		assert.Equal(t, want, err)
	})

	t.Run("PublishWithAttributesWithRetry runs publishing if the error was Throttled using defaults", func(t *testing.T) {
		repeatThrottlingErrorSetup(t, snsClient, publisher, want, true)

		err := publisher.PublishWithAttributesWithRetry("test message", map[string]string{
			"X-Correlation-Id": dummyUUID,
		})

		assert.NotNil(t, err)
		assert.Equal(t, want, err)
	})
}

func TestTopicPublisher_PublishEventWithRetry(t *testing.T) {
	snsClient, publisher, topicName, _ := publisherTestSetup(t)
	want := errors.New("non throttle error")

	t.Run("PublishEventWithRetry runs publishing if the error was Throttled using exponential backoff", func(t *testing.T) {
		repeatEventThrottlingErrorSetup(t, snsClient, publisher, topicName, want, false)

		err := publisher.PublishEventWithRetry("test message", WithExponentialBackoff(3, 1*time.Millisecond))

		assert.NotNil(t, err)
		assert.Equal(t, want, err)
	})

	t.Run("PublishEventWithRetry runs publishing if the error was Throttled using constant time change", func(t *testing.T) {
		repeatEventThrottlingErrorSetup(t, snsClient, publisher, topicName, want, false)

		err := publisher.PublishEventWithRetry("test message", WithConstant(3, 1*time.Millisecond))

		assert.NotNil(t, err)
		assert.Equal(t, want, err)
	})

	t.Run("PublishEventWithRetry runs publishing if the error was Throttled using defaults", func(t *testing.T) {
		repeatEventThrottlingErrorSetup(t, snsClient, publisher, topicName, want, false)

		err := publisher.PublishEventWithRetry("test message")

		assert.NotNil(t, err)
		assert.Equal(t, want, err)
	})
}

func TestTopicPublisher_PublishEventWithAttributesWithRetry(t *testing.T) {
	snsClient, publisher, topicName, _ := publisherTestSetup(t)
	want := errors.New("non throttle error")

	t.Run("PublishEventWithAttributesWithRetry runs publishing if the error was Throttled using exponential backoff", func(t *testing.T) {
		repeatEventThrottlingErrorSetup(t, snsClient, publisher, topicName, want, true)

		err := publisher.PublishEventWithAttributesWithRetry("test message", map[string]string{
			"X-Correlation-Id": DummyUUID,
		}, WithExponentialBackoff(3, 1*time.Millisecond))

		assert.NotNil(t, err)
		assert.Equal(t, want, err)
	})

	t.Run("PublishEventWithAttributesWithRetry runs publishing if the error was Throttled using constant time change", func(t *testing.T) {
		repeatEventThrottlingErrorSetup(t, snsClient, publisher, topicName, want, true)

		err := publisher.PublishEventWithAttributesWithRetry("test message", map[string]string{
			"X-Correlation-Id": DummyUUID,
		}, WithConstant(3, 1*time.Millisecond))

		assert.NotNil(t, err)
		assert.Equal(t, want, err)
	})

	t.Run("PublishEventWithAttributesWithRetry runs publishing if the error was Throttled using defaults", func(t *testing.T) {
		repeatEventThrottlingErrorSetup(t, snsClient, publisher, topicName, want, true)

		err := publisher.PublishEventWithAttributesWithRetry("test message", map[string]string{
			"X-Correlation-Id": DummyUUID,
		})

		assert.NotNil(t, err)
		assert.Equal(t, want, err)
	})
}

func publisherTestSetup(t *testing.T) (*mock.MockISNSClient, *TopicPublisher, string, string) {
	t.Helper()
	snsClient := mock.NewMockISNSClient(gomock.NewController(t))
	topicName := "test-topic"
	topicArn := "arn:test-topic"

	publisher := &TopicPublisher{
		snsClient: snsClient,
		topicName: topicName,
		topicArn:  topicArn,
	}

	return snsClient, publisher, topicName, topicArn

}

func repeatThrottlingErrorSetup(t *testing.T, snsClient *mock.MockISNSClient, publisher *TopicPublisher, expectedErrorMessage error, withAttributes bool) {
	ctx := context.Background()
	t.Helper()
	if withAttributes {
		snsClient.
			EXPECT().
			Publish(ctx, &sns.PublishInput{
				Message:  aws.String("test message"),
				TopicArn: aws.String(publisher.topicArn),
				MessageAttributes: map[string]types.MessageAttributeValue{
					"X-Correlation-Id": {StringValue: &DummyUUID},
				}}).
			Return(&sns.PublishOutput{}, &types.ThrottledException{}).Times(2)

		snsClient.
			EXPECT().
			Publish(ctx, &sns.PublishInput{
				Message:  aws.String("test message"),
				TopicArn: aws.String(publisher.topicArn),
				MessageAttributes: map[string]types.MessageAttributeValue{
					"X-Correlation-Id": {StringValue: &DummyUUID},
				}}).
			Return(&sns.PublishOutput{}, expectedErrorMessage)

	} else {
		snsClient.
			EXPECT().
			Publish(ctx, &sns.PublishInput{
				Message:  aws.String("test message"),
				TopicArn: aws.String(publisher.topicArn),
			}).
			Return(&sns.PublishOutput{}, &types.ThrottledException{}).Times(2)

		snsClient.
			EXPECT().
			Publish(ctx, &sns.PublishInput{
				Message:  aws.String("test message"),
				TopicArn: aws.String(publisher.topicArn),
			}).
			Return(&sns.PublishOutput{}, expectedErrorMessage)
	}
}

func repeatEventThrottlingErrorSetup(t *testing.T, snsClient *mock.MockISNSClient, publisher *TopicPublisher, topicName string, expectedErrorMessage error, withAttributes bool) {
	t.Helper()
	ctx := context.Background()
	if withAttributes {
		snsClient.
			EXPECT().
			Publish(ctx, &sns.PublishInput{
				Message:  aws.String("test message"),
				TopicArn: aws.String(publisher.topicArn),
				Subject:  aws.String(topicName),
				MessageAttributes: map[string]types.MessageAttributeValue{
					"X-Correlation-Id": {StringValue: &DummyUUID},
				}}).
			Return(&sns.PublishOutput{}, &types.ThrottledException{}).Times(2)

		snsClient.
			EXPECT().
			Publish(ctx, &sns.PublishInput{
				Message:  aws.String("test message"),
				TopicArn: aws.String(publisher.topicArn),
				Subject:  aws.String(topicName),
				MessageAttributes: map[string]types.MessageAttributeValue{
					"X-Correlation-Id": {StringValue: &DummyUUID},
				}}).
			Return(&sns.PublishOutput{}, expectedErrorMessage)
	} else {
		snsClient.
			EXPECT().
			Publish(ctx, &sns.PublishInput{
				Message:  aws.String("test message"),
				TopicArn: aws.String(publisher.topicArn),
				Subject:  aws.String(topicName),
			}).
			Return(&sns.PublishOutput{}, &types.ThrottledException{}).Times(2)

		snsClient.
			EXPECT().
			Publish(ctx, &sns.PublishInput{
				Message:  aws.String("test message"),
				TopicArn: aws.String(publisher.topicArn),
				Subject:  aws.String(topicName),
			}).
			Return(&sns.PublishOutput{}, expectedErrorMessage)
	}
}
