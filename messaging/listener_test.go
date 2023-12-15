package zaws

import (
	"ammyy9908/go-common-libraries/messaging/mock"
	"context"
	"errors"
	"testing"
	"time"

	"ammyy9908/go-common-libraries/gracefulshutdown"
	"ammyy9908/go-common-libraries/logger"

	"go.uber.org/zap"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func getTestListener() *SQSListener {
	log := logger.New(logger.Debug)
	ch := make(chan struct{})
	gsm := gracefulshutdown.NewManager(log, ch)

	return &SQSListener{
		queueName:                 "test-queue",
		queueURL:                  aws.String("test-queue.com"),
		sqsClient:                 nil,
		logger:                    log,
		handler:                   nil,
		gracefulShutdownManager:   gsm,
		receiveMessageWaitSeconds: 1,
		maxNumberOfMessages:       1,
	}
}

func TestNewListener(t *testing.T) {
	t.Run("NewListener returns a listener", func(t *testing.T) {
		testLogger := zap.NewNop().Sugar()

		listener, err := NewListener("test-queue", "ap-south-1", ListenerConfig{
			Logger:                    testLogger,
			Handler:                   nil,
			GracefulShutdownManager:   nil,
			ReceiveMessageWaitSeconds: 20,
			MaxNumberOfMessages:       10,
		})

		assert.NotEmpty(t, listener)
		assert.Nil(t, err)
	})

	t.Run("NewListener returns error when given region is not valid", func(t *testing.T) {
		testLogger := zap.NewNop().Sugar()

		listener, err := NewListener("test-queue", "bad-region", ListenerConfig{
			Logger:                    testLogger,
			Handler:                   nil,
			GracefulShutdownManager:   nil,
			ReceiveMessageWaitSeconds: 20,
			MaxNumberOfMessages:       10,
		})

		assert.Empty(t, listener)
		assert.NotNil(t, err)
	})
}

func TestSQSListener_poll(t *testing.T) {
	ctx := context.Background()
	var mockController = gomock.NewController(t)
	t.Run("poll should stop processing when it receives a value in the shutdown channel", func(t *testing.T) {
		sut := getTestListener()
		sqsClient := mock.NewMockISQSClient(mockController)
		handler := mock.NewMockMessageHandler(mockController)
		sut.handler = handler
		sut.sqsClient = sqsClient
		go func() {
			time.Sleep(time.Second * 1)
			sut.gracefulShutdownManager.ShutdownChannel <- 1
		}()
		receiveMessageInput := sqs.ReceiveMessageInput{}
		receiveMessageResponse := sqs.ReceiveMessageOutput{}
		sqsClient.
			EXPECT().
			ReceiveMessage(ctx, &receiveMessageInput).
			Return(&receiveMessageResponse, nil).
			AnyTimes()

		sut.poll(&receiveMessageInput)

		// Since we have called poll function synchronously (without a go routine), we guarantee that it has exited the for loop here.
		handler.EXPECT().Handle(gomock.Any()).Times(0)
	})

	t.Run("poll should handle message when it receives a message in the listener and deletes processed messages", func(t *testing.T) {
		sut := getTestListener()
		sqsClient := mock.NewMockISQSClient(mockController)
		handler := mock.NewMockMessageHandler(mockController)
		sut.handler = handler
		sut.sqsClient = sqsClient

		go func() {
			time.Sleep(time.Second * 1)
			sut.gracefulShutdownManager.ShutdownChannel <- 1
		}()

		receiveMessageInput := sqs.ReceiveMessageInput{}
		receiveMessageResponse := sqs.ReceiveMessageOutput{}
		message := types.Message{}
		var messages []types.Message
		messages = append(messages, message)
		outputResponseWithMessage := sqs.ReceiveMessageOutput{Messages: messages}

		first := sqsClient.
			EXPECT().
			ReceiveMessage(ctx, gomock.Any()).
			Return(&outputResponseWithMessage, nil)
		second := sqsClient.
			EXPECT().
			ReceiveMessage(ctx, &receiveMessageInput).
			Return(&receiveMessageResponse, nil).
			AnyTimes()
		gomock.InOrder(first, second)

		handler.
			EXPECT().
			Handle(message).
			Return(nil)
		sqsClient.
			EXPECT().
			DeleteMessage(ctx, gomock.Any()).
			Return(&sqs.DeleteMessageOutput{}, nil)

		sut.poll(&receiveMessageInput)

		// Since we have called poll function synchronously (without a go routine), we guarantee that it has exited the for loop here.

	})

	t.Run("poll should not delete message when handler returns error", func(t *testing.T) {
		sut := getTestListener()
		sqsClient := mock.NewMockISQSClient(mockController)
		handler := mock.NewMockMessageHandler(mockController)
		sut.handler = handler
		sut.sqsClient = sqsClient
		go func() {
			time.Sleep(time.Second * 1)
			sut.gracefulShutdownManager.ShutdownChannel <- 1
		}()
		receiveMessageInput := sqs.ReceiveMessageInput{}
		receiveMessageResponse := sqs.ReceiveMessageOutput{}
		message := types.Message{}
		var messages []types.Message
		messages = append(messages, message)
		outputResponseWithMessage := sqs.ReceiveMessageOutput{Messages: messages}
		first := sqsClient.
			EXPECT().
			ReceiveMessage(ctx, gomock.Any()).
			Return(&outputResponseWithMessage, nil)
		second := sqsClient.
			EXPECT().
			ReceiveMessage(ctx, &receiveMessageInput).
			Return(&receiveMessageResponse, nil).
			AnyTimes()

		gomock.InOrder(first, second)

		handlerErr := errors.New("random error")
		handler.
			EXPECT().
			Handle(message).
			Return(handlerErr)
		sqsClient.
			EXPECT().
			DeleteMessage(ctx, gomock.Any()).
			Times(0)
		sut.poll(&receiveMessageInput)

		// Since we have called poll function synchronously (without a go routine), we guarantee that it has exited the for loop here.

	})
}

func TestSQSListener_deleteMessage(t *testing.T) {
	ctx := context.Background()
	l := getTestListener()
	sqsClient := mock.NewMockISQSClient(gomock.NewController(t))

	msg := types.Message{
		Body:          aws.String("test"),
		ReceiptHandle: aws.String("test handle"),
	}

	t.Run("deleteMessage deletes the received message and does not return an error when given a processed message", func(t *testing.T) {
		sqsClient.
			EXPECT().
			DeleteMessage(ctx, &sqs.DeleteMessageInput{
				QueueUrl:      l.queueURL,
				ReceiptHandle: aws.String("test handle"),
			}).
			Return(&sqs.DeleteMessageOutput{}, nil)

		l.sqsClient = sqsClient

		err := l.deleteMessage(msg)

		assert.Nil(t, err)
	})

	t.Run("deleteMessage returns an error when it was unsuccessful", func(t *testing.T) {
		sqsClient.
			EXPECT().
			DeleteMessage(ctx, &sqs.DeleteMessageInput{
				QueueUrl:      l.queueURL,
				ReceiptHandle: aws.String("test handle"),
			}).
			Return(&sqs.DeleteMessageOutput{}, errors.New("delete error"))

		l.sqsClient = sqsClient

		err := l.deleteMessage(msg)

		assert.NotNil(t, err)
	})
}
