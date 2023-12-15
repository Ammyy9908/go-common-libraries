package zaws

import (
	"ammyy9908/go-common-libraries/messaging/mock"
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestNewQueuePublisher(t *testing.T) {
	badRegion := "bad-region"
	testQueue := "test-queue"
	_, err := NewQueuePublisher(badRegion, testQueue)

	assert.NotNil(t, err)
}

func TestNewQueuePublisherWithConfig(t *testing.T) {

}

func TestQueuePublisher_Publish(t *testing.T) {
	sqsClient := mock.NewMockISQSClient(gomock.NewController(t))
	ctx := context.Background()

	publisher := &QueuePublisher{
		sqsClient: sqsClient,
		queueName: "test-queue",
		queueURL:  "www.test-queue.com",
	}

	t.Run("Publish sends a message and returns no error when aws does not return an error", func(t *testing.T) {
		sqsClient.
			EXPECT().
			SendMessage(ctx, &sqs.SendMessageInput{
				MessageBody: aws.String("test message"),
				QueueUrl:    aws.String(publisher.queueURL),
			}).
			Return(&sqs.SendMessageOutput{}, nil)

		err := publisher.Publish("test message")

		assert.Nil(t, err)
	})

	t.Run("Publish returns an error when the message sending returns an error", func(t *testing.T) {
		sqsClient.
			EXPECT().
			SendMessage(ctx, &sqs.SendMessageInput{
				MessageBody: aws.String("test message"),
				QueueUrl:    aws.String(publisher.queueURL),
			}).
			Return(&sqs.SendMessageOutput{}, errors.New("test error"))

		err := publisher.Publish("test message")

		assert.NotNil(t, err)
	})
}

func TestQueuePublisher_PublishWithAttributes(t *testing.T) {
	sqsClient := mock.NewMockISQSClient(gomock.NewController(t))
	ctx := context.Background()

	publisher := &QueuePublisher{
		sqsClient: sqsClient,
		queueName: "test-queue",
		queueURL:  "www.test-queue.com",
	}

	t.Run("PublishWithAttributes sends a message and returns no error when aws does not return an error", func(t *testing.T) {
		dummyUUID := "<some-uuid>"
		sqsClient.
			EXPECT().
			SendMessage(ctx, &sqs.SendMessageInput{
				MessageBody: aws.String("test message"),
				QueueUrl:    aws.String(publisher.queueURL),
				MessageAttributes: map[string]types.MessageAttributeValue{
					"X-Correlation-Id": {StringValue: &dummyUUID},
				},
			}).
			Return(&sqs.SendMessageOutput{}, nil)

		err := publisher.PublishWithAttributes("test message", map[string]string{
			"X-Correlation-Id": dummyUUID,
		})

		assert.Nil(t, err)
	})

	t.Run("PublishWithAttributes returns an error when the message sending returns an error", func(t *testing.T) {
		dummyUUID := "<some-uuid>"
		sqsClient.
			EXPECT().
			SendMessage(ctx, &sqs.SendMessageInput{
				MessageBody: aws.String("test message"),
				QueueUrl:    aws.String(publisher.queueURL),
				MessageAttributes: map[string]types.MessageAttributeValue{
					"X-Correlation-Id": {StringValue: &dummyUUID},
				},
			}).
			Return(&sqs.SendMessageOutput{}, errors.New("test error"))

		err := publisher.PublishWithAttributes("test message", map[string]string{
			"X-Correlation-Id": dummyUUID,
		})

		assert.NotNil(t, err)
	})
}
