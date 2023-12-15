package zaws

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

const (
	ErrMissingTags        = "missing tags"
	errWaitTimeMoreThan20 = "wait time cannot be more than 20"
	errWaitTimeNegative   = "wait time cannot be less than 0"
)

type IManager interface {
	CreateQueue(queueName string, config QueueConfig) (*sqs.CreateQueueOutput, error)
	CreateTopic(topicName string, tags map[string]string) (*sns.CreateTopicOutput, error)
	SubscribeQueueToTopic(queueName, topicName string, raw bool) error
	GetTopicArn(topicName string) (string, error)
	GetQueueArn(queueName string) (string, error)
	SubscribeQueueToTopicV2(queueName, topicName string, raw bool)
}

type Manager struct {
	snsClient ISNSClient
	sqsClient ISQSClient
}

func NewManager(region string) (*Manager, error) {
	cfg, err := awsConfig.LoadDefaultConfig(context.Background(), awsConfig.WithRegion(region))
	if err != nil {
		return nil, err
	}

	return NewManagerWithConfig(cfg)
}

func NewManagerWithConfig(config aws.Config) (*Manager, error) {
	sqsClient := sqs.NewFromConfig(config)
	snsClient := sns.NewFromConfig(config)

	manager := &Manager{
		snsClient: snsClient,
		sqsClient: sqsClient,
	}

	return manager, nil
}

func (m *Manager) CreateQueue(queueName string, config QueueConfig) (*sqs.CreateQueueOutput, error) {
	err := validateQueueName(queueName)
	if err != nil {
		return nil, err
	}

	err = validateReceiveTimeWaitSeconds(config.ReceiveWaitTimeSeconds)
	if err != nil {
		return nil, err
	}

	result, err := setupNewQueue(m.sqsClient, queueName, config)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (m *Manager) CreateTopic(topicName string, tags map[string]string) (*sns.CreateTopicOutput, error) {
	ctx := context.Background()
	topicTags := createTopicTags(tags)
	if topicTags == nil {
		return nil, errors.New(ErrMissingTags)
	}

	result, err := m.snsClient.CreateTopic(ctx, &sns.CreateTopicInput{
		Name: aws.String(topicName),
		Tags: topicTags,
	})
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (m *Manager) GetTopicArn(topicName string) (string, error) {
	topicARN, err := getTopicArn(m.snsClient, topicName)
	if err != nil {
		return "", err
	}
	return topicARN, nil
}

func (m *Manager) GetQueueArn(queueName string) (string, error) {
	queueURL, err := GetQueueURL(m.sqsClient, queueName)
	if err != nil {
		return "", err
	}

	queueARN, err := getQueueArn(m.sqsClient, queueURL)
	if err != nil {
		return "", err
	}
	return queueARN, nil
}

func (m *Manager) SubscribeQueueToTopic(queueName, topicName string, raw bool) error {
	topicARN, err := getTopicArn(m.snsClient, topicName)
	if err != nil {
		return err
	}

	queueURL, err := GetQueueURL(m.sqsClient, queueName)
	if err != nil {
		return err
	}

	queueARN, err := getQueueArn(m.sqsClient, queueURL)
	if err != nil {
		return err
	}
	_, err = m.subscribe(topicARN, queueARN, raw)
	if err != nil {
		return err
	}

	policy, err := getPolicyContent(m.sqsClient, queueURL, topicARN)
	if err != nil {
		return err
	}
	err = setTopicPolicy(m.sqsClient, queueURL, policy)
	if err != nil {
		return err
	}
	return nil
}

func (m *Manager) SubscribeQueueToTopicV2(queueName, topicName string, raw bool) error {
	topicARN, err := getTopicArn(m.snsClient, topicName)
	if err != nil {
		return err
	}

	queueURL, err := GetQueueURL(m.sqsClient, queueName)
	if err != nil {
		return err
	}

	queueARN, err := getQueueArn(m.sqsClient, queueURL)
	if err != nil {
		return err
	}
	_, err = m.subscribe(topicARN, queueARN, raw)
	if err != nil {
		return err
	}

	return nil
}

func (m *Manager) subscribe(topicARN, queueARN string, raw bool) (*sns.SubscribeOutput, error) {
	ctx := context.Background()
	rawMessageDelivery := "false"

	if raw {
		rawMessageDelivery = "true"
	}

	subscription, err := m.snsClient.Subscribe(ctx, &sns.SubscribeInput{
		TopicArn: aws.String(topicARN),
		Protocol: aws.String("sqs"),
		Endpoint: aws.String(queueARN),
		Attributes: map[string]string{
			"RawMessageDelivery": rawMessageDelivery,
		},
	})
	if err != nil {
		return nil, err
	}
	return subscription, nil
}

func validateReceiveTimeWaitSeconds(receiveTimeWait int) error {
	if receiveTimeWait > 20 {
		return errors.New(errWaitTimeMoreThan20)
	}
	if receiveTimeWait < 0 {
		return errors.New(errWaitTimeNegative)
	}

	return nil
}
