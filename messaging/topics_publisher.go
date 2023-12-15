package zaws

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/sns/types"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sns"
)

type ITopicsPublisher interface {
	Publish(topicName, message string) error
	PublishWithAttributes(topicName, message string, attributes map[string]string) error
	PublishEvent(topicName, subject, message string) error
	PublishEventWithAttributes(topicName, subject, message string, attributes map[string]string) error
}

type TopicsPublisher struct {
	snsClient   ISNSClient
	topicsCache map[string]string
}

func NewTopicsPublisher(region string) (*TopicsPublisher, error) {
	cfg, err := awsConfig.LoadDefaultConfig(context.Background(), awsConfig.WithRegion(region))
	if err != nil {
		return nil, err
	}

	return NewTopicsPublisherWithConfig(cfg)
}

func NewTopicsPublisherWithConfig(cfg aws.Config) (*TopicsPublisher, error) {
	snsClient := sns.NewFromConfig(cfg)
	cache := make(map[string]string)

	return &TopicsPublisher{
		snsClient:   snsClient,
		topicsCache: cache,
	}, nil
}

func (p *TopicsPublisher) Publish(topicName, message string) error {
	ctx := context.Background()
	topicArn, err := p.getTopicArn(topicName)
	if err != nil {
		return err
	}

	_, err = p.snsClient.Publish(ctx, &sns.PublishInput{
		Message:  aws.String(message),
		TopicArn: aws.String(topicArn),
	})

	if err != nil {
		return err
	}
	return nil
}

func (p *TopicsPublisher) PublishWithAttributes(topicName, message string, attributes map[string]string) error {
	ctx := context.Background()
	topicArn, err := p.getTopicArn(topicName)
	if err != nil {
		return err
	}
	messageAttributesMap := make(map[string]types.MessageAttributeValue)
	for key, val := range attributes {
		messageAttributesMap[key] = types.MessageAttributeValue{StringValue: &val}
	}

	_, err = p.snsClient.Publish(ctx, &sns.PublishInput{
		Message:           aws.String(message),
		TopicArn:          aws.String(topicArn),
		MessageAttributes: messageAttributesMap,
	})

	if err != nil {
		return err
	}
	return nil
}

func (p *TopicsPublisher) PublishEvent(topicName, subject, message string) error {
	ctx := context.Background()
	topicArn, err := p.getTopicArn(topicName)
	if err != nil {
		return err
	}

	_, err = p.snsClient.Publish(ctx, &sns.PublishInput{
		Message:  aws.String(message),
		TopicArn: aws.String(topicArn),
		Subject:  aws.String(subject),
	})
	return err
}

func (p *TopicsPublisher) PublishEventWithAttributes(topicName, subject, message string, attributes map[string]string) error {
	ctx := context.Background()
	topicArn, err := p.getTopicArn(topicName)
	if err != nil {
		return err
	}
	messageAttributesMap := make(map[string]types.MessageAttributeValue)
	for key, val := range attributes {
		messageAttributesMap[key] = types.MessageAttributeValue{StringValue: &val}
	}

	_, err = p.snsClient.Publish(ctx, &sns.PublishInput{
		Message:           aws.String(message),
		TopicArn:          aws.String(topicArn),
		Subject:           aws.String(subject),
		MessageAttributes: messageAttributesMap,
	})
	return err
}

func (p *TopicsPublisher) getTopicArn(topicName string) (string, error) {
	topicArn, ok := p.topicsCache[topicName]
	if !ok {
		resultArn, err := getTopicArn(p.snsClient, topicName)
		if err != nil {
			return "", err
		}
		p.topicsCache[topicName] = resultArn
		topicArn = resultArn
	}

	return topicArn, nil
}
