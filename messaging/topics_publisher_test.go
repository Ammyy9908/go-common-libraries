package zaws

import (
	"context"
	"errors"
	"testing"

	"github.com/ammyy9908/go-common-libraries/messaging/mock"

	"github.com/aws/aws-sdk-go-v2/service/sns/types"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func setup(t *testing.T) (*mock.MockISNSClient, *TopicsPublisher, string) {
	t.Helper()

	snsClient := mock.NewMockISNSClient(gomock.NewController(t))

	topicsCache := make(map[string]string)
	testTopic := "test-topic-1"
	topicsCache[testTopic] = "test-arn"

	publisher := &TopicsPublisher{
		snsClient:   snsClient,
		topicsCache: topicsCache,
	}

	return snsClient, publisher, testTopic
}

func TestNewTopicsPublisher(t *testing.T) {
	t.Run("NewTopicsPublisher returns new topic publisher", func(t *testing.T) {
		publisher, err := NewTopicsPublisher("ap-south-1")

		assert.NotEmpty(t, publisher)
		assert.Nil(t, err)
	})
}

func TestTopicsPublisher_Publish(t *testing.T) {
	ctx := context.Background()

	t.Run("Publish sends a message to the topic when aws publish gives no error", func(t *testing.T) {
		snsClient, publisher, testTopic := setup(t)
		snsClient.
			EXPECT().
			Publish(ctx, &sns.PublishInput{
				Message:  aws.String("test message"),
				TopicArn: aws.String("test-arn"),
			}).
			Return(&sns.PublishOutput{}, nil)

		err := publisher.Publish(testTopic, "test message")

		assert.Nil(t, err)

	})

	t.Run("Publish returns an error when the publishing causes an error", func(t *testing.T) {
		snsClient, publisher, testTopic := setup(t)
		snsClient.
			EXPECT().
			Publish(ctx, &sns.PublishInput{
				Message:  aws.String("test message"),
				TopicArn: aws.String(publisher.topicsCache[testTopic]),
			}).
			Return(&sns.PublishOutput{}, errors.New("test error"))

		err := publisher.Publish(testTopic, "test message")

		assert.NotNil(t, err)
	})

	t.Run("Publish publishes to a topic whose getTopicArn is not in cache by retrieving it first", func(t *testing.T) {
		snsClient, publisher, _ := setup(t)
		testTopic2 := "test-topic-2"
		testTopic2Arn := "test-topic-arn2"

		snsClient.
			EXPECT().
			CreateTopic(ctx, &sns.CreateTopicInput{
				Name: aws.String(testTopic2),
			}).
			Return(&sns.CreateTopicOutput{TopicArn: aws.String(testTopic2Arn)}, nil)

		snsClient.
			EXPECT().
			Publish(ctx, &sns.PublishInput{
				Message:  aws.String("test message"),
				TopicArn: aws.String(testTopic2Arn),
			}).
			Return(&sns.PublishOutput{}, nil)

		err := publisher.Publish(testTopic2, "test message")

		assert.Nil(t, err)
		assert.Equal(t, publisher.topicsCache[testTopic2], testTopic2Arn)
	})

	t.Run("Publish returns an error if the method fails to fetch topic Arn", func(t *testing.T) {
		snsClient, publisher, _ := setup(t)
		testTopic3 := "test-topic-3"
		testTopic3Arn := "test-topic-arn3"

		snsClient.
			EXPECT().
			CreateTopic(ctx, &sns.CreateTopicInput{
				Name: aws.String(testTopic3),
			}).
			Return(&sns.CreateTopicOutput{TopicArn: aws.String(testTopic3Arn)}, errors.New("topic creation error"))

		err := publisher.Publish(testTopic3, "test message")
		_, ok := publisher.topicsCache[testTopic3]

		assert.NotNil(t, err)
		assert.False(t, ok)
	})

	t.Run("Publish returns error when publish causes an error, but caches the topic arn", func(t *testing.T) {
		snsClient, publisher, _ := setup(t)
		testTopic4 := "test-topic-4"
		testTopic4Arn := "test-topic-arn4"

		snsClient.
			EXPECT().
			CreateTopic(ctx, &sns.CreateTopicInput{
				Name: aws.String(testTopic4),
			}).
			Return(&sns.CreateTopicOutput{TopicArn: aws.String(testTopic4Arn)}, nil)

		snsClient.
			EXPECT().
			Publish(ctx, &sns.PublishInput{
				Message:  aws.String("test message"),
				TopicArn: aws.String(testTopic4Arn),
			}).
			Return(&sns.PublishOutput{}, errors.New("publishing error"))

		err := publisher.Publish(testTopic4, "test message")

		assert.NotNil(t, err)
		assert.Equal(t, publisher.topicsCache[testTopic4], testTopic4Arn)
	})
}

func TestTopicsPublisher_PublishEvent(t *testing.T) {
	ctx := context.Background()

	t.Run("PublishEvent sends a message to the topic when aws publish gives no error", func(t *testing.T) {
		snsClient, publisher, testTopic := setup(t)
		snsClient.
			EXPECT().
			Publish(ctx, &sns.PublishInput{
				Message:  aws.String("test message"),
				TopicArn: aws.String("test-arn"),
				Subject:  aws.String(testTopic),
			}).
			Return(&sns.PublishOutput{}, nil)

		err := publisher.PublishEvent(testTopic, testTopic, "test message")

		assert.Nil(t, err)
	})

	t.Run("PublishEvent returns an error when the publishing causes an error", func(t *testing.T) {
		snsClient, publisher, testTopic := setup(t)
		snsClient.
			EXPECT().
			Publish(ctx, &sns.PublishInput{
				Message:  aws.String("test message"),
				TopicArn: aws.String("test-arn"),
				Subject:  aws.String(testTopic),
			}).
			Return(&sns.PublishOutput{}, errors.New("test error"))

		err := publisher.PublishEvent(testTopic, testTopic, "test message")

		assert.NotNil(t, err)
	})

	t.Run("PublishEvent publishes to a topic whose getTopicArn is not in cache by retrieving it first", func(t *testing.T) {
		snsClient, publisher, _ := setup(t)
		testTopic2 := "test-topic-2"
		testTopic2Arn := "test-topic-arn2"

		snsClient.
			EXPECT().
			CreateTopic(ctx, &sns.CreateTopicInput{
				Name: aws.String(testTopic2),
			}).
			Return(&sns.CreateTopicOutput{TopicArn: aws.String(testTopic2Arn)}, nil)

		snsClient.
			EXPECT().
			Publish(ctx, &sns.PublishInput{
				Message:  aws.String("test message"),
				TopicArn: aws.String(testTopic2Arn),
				Subject:  aws.String(testTopic2),
			}).
			Return(&sns.PublishOutput{}, nil)

		err := publisher.PublishEvent(testTopic2, testTopic2, "test message")

		assert.Nil(t, err)
		assert.Equal(t, publisher.topicsCache[testTopic2], testTopic2Arn)
	})

	t.Run("PublishEvent returns an error if the method fails to fetch topic Arn", func(t *testing.T) {
		snsClient, publisher, _ := setup(t)
		testTopic3 := "test-topic-3"
		testTopic3Arn := "test-topic-arn3"

		snsClient.
			EXPECT().
			CreateTopic(ctx, &sns.CreateTopicInput{
				Name: aws.String(testTopic3),
			}).
			Return(&sns.CreateTopicOutput{TopicArn: aws.String(testTopic3Arn)}, errors.New("topic creation error"))

		err := publisher.PublishEvent(testTopic3, testTopic3, "test message")
		_, ok := publisher.topicsCache[testTopic3]

		assert.NotNil(t, err)
		assert.False(t, ok)
	})

	t.Run("PublishEvent returns error when publish causes an error, but caches the topic arn", func(t *testing.T) {
		snsClient, publisher, _ := setup(t)
		testTopic4 := "test-topic-4"
		testTopic4Arn := "test-topic-arn4"

		snsClient.
			EXPECT().
			CreateTopic(ctx, &sns.CreateTopicInput{
				Name: aws.String(testTopic4),
			}).
			Return(&sns.CreateTopicOutput{TopicArn: aws.String(testTopic4Arn)}, nil)

		snsClient.
			EXPECT().
			Publish(ctx, &sns.PublishInput{
				Message:  aws.String("test message"),
				TopicArn: aws.String(testTopic4Arn),
				Subject:  aws.String(testTopic4),
			}).
			Return(&sns.PublishOutput{}, errors.New("publishing error"))

		err := publisher.PublishEvent(testTopic4, testTopic4, "test message")

		assert.NotNil(t, err)
		assert.Equal(t, publisher.topicsCache[testTopic4], testTopic4Arn)
	})
}

func TestTopicsPublisher_getTopicArn(t *testing.T) {
	ctx := context.Background()

	t.Run("getTopicArn fetches topic arn and saves it in topicCache map", func(t *testing.T) {
		snsClient, publisher, _ := setup(t)
		topicName := "test-arn-1"
		topicArn := "test-arn-1"
		snsClient.
			EXPECT().
			CreateTopic(ctx, &sns.CreateTopicInput{
				Name: aws.String(topicName),
			}).
			Return(&sns.CreateTopicOutput{TopicArn: aws.String(topicArn)}, nil)

		arn, err := publisher.getTopicArn(topicName)

		assert.Equal(t, topicArn, arn)
		assert.Equal(t, publisher.topicsCache[topicName], topicArn)
		assert.Nil(t, err)
	})

	t.Run("getTopicArn fetches existing topic arn without calling Service", func(t *testing.T) {
		_, publisher, topicName := setup(t)
		topicArn := "test-arn"

		arn, err := publisher.getTopicArn(topicName)

		assert.Equal(t, topicArn, arn)
		assert.Equal(t, publisher.topicsCache[topicName], topicArn)
		assert.Nil(t, err)
	})

	t.Run("getTopicArn returns an error if the topic arn cannot be retrieved", func(t *testing.T) {
		snsClient, publisher, _ := setup(t)
		topicName := "test-topic-2"
		snsClient.
			EXPECT().
			CreateTopic(ctx, &sns.CreateTopicInput{
				Name: aws.String(topicName),
			}).
			Return(&sns.CreateTopicOutput{}, errors.New("topic creation error"))

		arn, err := publisher.getTopicArn(topicName)
		_, ok := publisher.topicsCache[topicName]

		assert.Equal(t, "", arn)
		assert.False(t, ok)
		assert.NotNil(t, err)
	})
}

func TestTopicsPublisher_PublishWithAttributes(t *testing.T) {
	ctx := context.Background()
	testAttribute := "test-value"
	inputAttributes := map[string]string{"test-key": "test-value"}
	messageAttributesMap := make(map[string]types.MessageAttributeValue)
	messageAttributesMap["test-key"] = types.MessageAttributeValue{StringValue: &testAttribute}

	t.Run("PublishWithAttributes sends message and does not return an error", func(t *testing.T) {
		snsClient, publisher, topicName := setup(t)

		snsClient.EXPECT().
			Publish(ctx, &sns.PublishInput{
				Message:           aws.String("test-message"),
				TopicArn:          aws.String("test-arn"),
				MessageAttributes: messageAttributesMap,
			}).
			Return(&sns.PublishOutput{}, nil)

		err := publisher.PublishWithAttributes(topicName, "test-message", inputAttributes)

		assert.Nil(t, err)
	})

	t.Run("PublishWithAttributes returns error when topic arn returns an error", func(t *testing.T) {
		snsClient, publisher, _ := setup(t)
		testError := errors.New("bad topic")

		snsClient.EXPECT().
			CreateTopic(ctx, &sns.CreateTopicInput{
				Name: aws.String("bad-topic"),
			}).
			Return(&sns.CreateTopicOutput{}, testError)

		err := publisher.PublishWithAttributes("bad-topic", "test-message", inputAttributes)

		assert.Equal(t, testError, err)
	})

	t.Run("PublishWithAttributes returns error when message fails tu publish", func(t *testing.T) {
		snsClient, publisher, topicName := setup(t)
		testError := errors.New("publishing errors")

		snsClient.EXPECT().
			Publish(ctx, &sns.PublishInput{
				Message:           aws.String("test-message"),
				TopicArn:          aws.String("test-arn"),
				MessageAttributes: messageAttributesMap,
			}).
			Return(&sns.PublishOutput{}, testError)

		err := publisher.PublishWithAttributes(topicName, "test-message", inputAttributes)

		assert.Equal(t, testError, err)
	})
}

func TestTopicsPublisher_PublishEventWithAttributes(t *testing.T) {
	ctx := context.Background()
	testAttribute := "test-value"
	testSubject := "test-subject"
	inputAttributes := map[string]string{"test-key": "test-value"}
	messageAttributesMap := make(map[string]types.MessageAttributeValue)
	messageAttributesMap["test-key"] = types.MessageAttributeValue{StringValue: &testAttribute}

	t.Run("PublishWithAttributes sends message and does not return an error", func(t *testing.T) {
		snsClient, publisher, topicName := setup(t)

		snsClient.EXPECT().
			Publish(ctx, &sns.PublishInput{
				Message:           aws.String("test-message"),
				Subject:           aws.String(testSubject),
				TopicArn:          aws.String("test-arn"),
				MessageAttributes: messageAttributesMap,
			}).
			Return(&sns.PublishOutput{}, nil)

		err := publisher.PublishEventWithAttributes(topicName, testSubject, "test-message", inputAttributes)

		assert.Nil(t, err)
	})

	t.Run("PublishWithAttributes returns error when topic arn returns an error", func(t *testing.T) {
		snsClient, publisher, _ := setup(t)
		testError := errors.New("bad topic")

		snsClient.EXPECT().
			CreateTopic(ctx, &sns.CreateTopicInput{
				Name: aws.String("bad-topic"),
			}).
			Return(&sns.CreateTopicOutput{}, testError)

		err := publisher.PublishEventWithAttributes("bad-topic", testSubject, "test-message", inputAttributes)

		assert.Equal(t, testError, err)
	})

	t.Run("PublishWithAttributes returns error when message fails tu publish", func(t *testing.T) {
		snsClient, publisher, topicName := setup(t)
		testError := errors.New("publishing errors")

		snsClient.EXPECT().
			Publish(ctx, &sns.PublishInput{
				Message:           aws.String("test-message"),
				Subject:           aws.String(testSubject),
				TopicArn:          aws.String("test-arn"),
				MessageAttributes: messageAttributesMap,
			}).
			Return(&sns.PublishOutput{}, testError)

		err := publisher.PublishEventWithAttributes(topicName, testSubject, "test-message", inputAttributes)

		assert.Equal(t, testError, err)
	})
}
