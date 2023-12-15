package zaws

import (
	"ammyy9908/go-common-libraries/gracefulshutdown"
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"go.uber.org/zap"
)

type ISQSListener interface {
	Listen()
}

type SQSListener struct {
	queueName                 string
	queueURL                  *string
	sqsClient                 ISQSClient
	logger                    *zap.SugaredLogger
	handler                   MessageHandler
	gracefulShutdownManager   *gracefulshutdown.Manager
	receiveMessageWaitSeconds int
	maxNumberOfMessages       int
}

type ListenerConfig struct {
	Logger                    *zap.SugaredLogger
	Handler                   MessageHandler
	GracefulShutdownManager   *gracefulshutdown.Manager
	ReceiveMessageWaitSeconds int
	MaxNumberOfMessages       int
}

func NewListener(queueName, region string, listenerConfig ListenerConfig) (*SQSListener, error) {
	cfg, err := awsConfig.LoadDefaultConfig(context.Background(), awsConfig.WithRegion(region))
	if err != nil {
		return nil, err
	}

	listener, err := NewListenerWithConfig(queueName, cfg, listenerConfig)
	if err != nil {
		return nil, err
	}

	return listener, nil
}

func NewListenerWithConfig(queueName string, cfg aws.Config, listenerConfig ListenerConfig) (*SQSListener, error) {
	sqsClient := sqs.NewFromConfig(cfg)

	queueURL, err := GetQueueURL(sqsClient, queueName)
	if err != nil {
		return nil, err
	}

	return &SQSListener{
		queueName:                 queueName,
		queueURL:                  &queueURL,
		sqsClient:                 sqsClient,
		logger:                    listenerConfig.Logger,
		handler:                   listenerConfig.Handler,
		gracefulShutdownManager:   listenerConfig.GracefulShutdownManager,
		receiveMessageWaitSeconds: listenerConfig.ReceiveMessageWaitSeconds,
		maxNumberOfMessages:       listenerConfig.MaxNumberOfMessages,
	}, nil
}

func (l *SQSListener) Listen() {
	retrievedMessagesRequest := &sqs.ReceiveMessageInput{
		QueueUrl:              l.queueURL,
		WaitTimeSeconds:       int32(l.receiveMessageWaitSeconds),
		MaxNumberOfMessages:   int32(l.maxNumberOfMessages),
		MessageAttributeNames: []string{"All"},
	}

	l.poll(retrievedMessagesRequest)
}

func (l *SQSListener) poll(request *sqs.ReceiveMessageInput) {
	ctx := context.Background()
	for {
		select {
		case <-l.gracefulShutdownManager.ShutdownChannel:
			l.gracefulShutdownManager.ShutdownWaitGroup.Wait()
			return
		default:
			retrieveMessageResponse, err := l.sqsClient.ReceiveMessage(ctx, request)
			if err != nil {
				l.logger.Error(err.Error())
				continue
			}
			if len(retrieveMessageResponse.Messages) < 1 {
				continue
			}
			for _, m := range retrieveMessageResponse.Messages {
				l.gracefulShutdownManager.ShutdownWaitGroup.Add(1)
				go func(message types.Message) {
					defer l.gracefulShutdownManager.ShutdownWaitGroup.Done()
					l.handleMessage(message)
				}(m)
			}
		}
	}
}

func (l *SQSListener) handleMessage(message types.Message) {
	err := l.handler.Handle(message)
	if err != nil {
		l.logger.Error(*message.Body)
		l.logger.Error(err.Error())
	} else {
		err = l.deleteMessage(message)
		if err != nil {
			l.logger.Error(err.Error())
		}
	}
}

func (l *SQSListener) deleteMessage(message types.Message) error {
	ctx := context.Background()
	_, err := l.sqsClient.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      l.queueURL,
		ReceiptHandle: message.ReceiptHandle,
	})
	if err != nil {
		return err
	}
	return nil
}
