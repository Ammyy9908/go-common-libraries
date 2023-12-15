package zaws

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/sns/types"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sns"
)

type ITopicPublisher interface {
	Publish(message string) error
	PublishWithAttributes(message string, attributes map[string]string) error
	PublishEvent(message string) error
	PublishEventWithAttributes(message string, attributes map[string]string) error
	PublishWithRetry(message string, opts ...Option) error
	PublishWithAttributesWithRetry(message string, attributes map[string]string, opts ...Option) error
	PublishEventWithRetry(message string, opts ...Option) error
	PublishEventWithAttributesWithRetry(message string, attributes map[string]string, opts ...Option) error
}

type TopicPublisher struct {
	snsClient ISNSClient
	topicName string
	topicArn  string
}

func NewTopicPublisher(region string, topicName string) (*TopicPublisher, error) {
	cfg, err := awsConfig.LoadDefaultConfig(context.Background(), awsConfig.WithRegion(region))
	if err != nil {
		return nil, err
	}

	publisher, err := NewTopicPublisherWithConfig(cfg, topicName)
	if err != nil {
		return nil, err
	}
	return publisher, nil
}

func NewTopicPublisherWithConfig(cfg aws.Config, topicName string) (*TopicPublisher, error) {
	snsClient := sns.NewFromConfig(cfg)

	topicArn, err := getTopicArn(snsClient, topicName)
	if err != nil {
		return nil, err
	}

	return &TopicPublisher{
		snsClient: snsClient,
		topicName: topicName,
		topicArn:  topicArn,
	}, nil
}

func (p *TopicPublisher) Publish(message string) error {
	ctx := context.Background()
	_, err := p.snsClient.Publish(ctx, &sns.PublishInput{
		Message:  aws.String(message),
		TopicArn: aws.String(p.topicArn),
	})
	return err
}

func (p *TopicPublisher) PublishWithAttributes(message string, attributes map[string]string) error {
	ctx := context.Background()
	messageAttributesMap := make(map[string]types.MessageAttributeValue)
	for key, val := range attributes {
		messageAttributesMap[key] = types.MessageAttributeValue{StringValue: &val}
	}
	_, err := p.snsClient.Publish(ctx, &sns.PublishInput{
		Message:           aws.String(message),
		TopicArn:          aws.String(p.topicArn),
		MessageAttributes: messageAttributesMap,
	})
	return err
}

func (p *TopicPublisher) PublishEvent(message string) error {
	ctx := context.Background()
	_, err := p.snsClient.Publish(ctx, &sns.PublishInput{
		Message:  aws.String(message),
		TopicArn: aws.String(p.topicArn),
		Subject:  aws.String(p.topicName),
	})
	return err
}

func (p *TopicPublisher) PublishEventWithAttributes(message string, attributes map[string]string) error {
	ctx := context.Background()
	messageAttributesMap := make(map[string]types.MessageAttributeValue)
	for key, val := range attributes {
		messageAttributesMap[key] = types.MessageAttributeValue{StringValue: &val}
	}
	_, err := p.snsClient.Publish(ctx, &sns.PublishInput{
		Message:           aws.String(message),
		TopicArn:          aws.String(p.topicArn),
		Subject:           aws.String(p.topicName),
		MessageAttributes: messageAttributesMap,
	})
	return err
}

func (p *TopicPublisher) PublishWithRetry(message string, opts ...Option) error {
	return Retry(p.Publish, message, opts...)
}

func (p *TopicPublisher) PublishWithAttributesWithRetry(message string, attributes map[string]string, opts ...Option) error {
	return RetryWithAttributes(p.PublishWithAttributes, message, attributes, opts...)
}

func (p *TopicPublisher) PublishEventWithRetry(message string, opts ...Option) error {
	return Retry(p.PublishEvent, message, opts...)
}

func (p *TopicPublisher) PublishEventWithAttributesWithRetry(message string, attributes map[string]string, opts ...Option) error {
	return RetryWithAttributes(p.PublishEventWithAttributes, message, attributes, opts...)
}
