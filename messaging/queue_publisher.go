package zaws

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

type IQueuePublisher interface {
	Publish(message string) error
	PublishWithAttributes(message string, attributes map[string]string) error
}

type QueuePublisher struct {
	sqsClient ISQSClient
	queueName string
	queueURL  string
}

func NewQueuePublisher(region string, queueName string) (*QueuePublisher, error) {
	cfg, err := awsConfig.LoadDefaultConfig(context.Background(), awsConfig.WithRegion(region))
	if err != nil {
		return nil, err
	}

	publisher, err := NewQueuePublisherWithConfig(cfg, queueName)
	if err != nil {
		return nil, err
	}
	return publisher, nil
}

func NewQueuePublisherWithConfig(cfg aws.Config, queueName string) (*QueuePublisher, error) {
	sqsClient := sqs.NewFromConfig(cfg)

	queueURL, err := GetQueueURL(sqsClient, queueName)
	if err != nil {
		return nil, err
	}

	return &QueuePublisher{
		sqsClient: sqsClient,
		queueName: queueName,
		queueURL:  queueURL,
	}, nil
}

func (p *QueuePublisher) Publish(message string) error {
	ctx := context.Background()
	_, err := p.sqsClient.SendMessage(ctx, &sqs.SendMessageInput{
		MessageBody: aws.String(message),
		QueueUrl:    aws.String(p.queueURL),
	})
	return err
}

func (p *QueuePublisher) PublishWithAttributes(message string, attributes map[string]string) error {
	ctx := context.Background()
	messageAttributesMap := make(map[string]types.MessageAttributeValue)
	for key, val := range attributes {
		messageAttributesMap[key] = types.MessageAttributeValue{StringValue: &val}
	}
	_, err := p.sqsClient.SendMessage(ctx, &sqs.SendMessageInput{
		MessageBody:       aws.String(message),
		QueueUrl:          aws.String(p.queueURL),
		MessageAttributes: messageAttributesMap,
	})
	return err
}
