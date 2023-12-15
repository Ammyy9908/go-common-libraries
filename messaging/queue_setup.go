package zaws

import (
	"context"
	"errors"
	"regexp"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/goccy/go-json"
)

const (
	ErrorQueueSuffix                   = "_ERROR"
	errQueueNameTooLong                = "queue name is too long"
	errNonAlphaNumericCharsInQueueName = "queue name contains non alphanumeric characters"
	errQueueNameEmpty                  = "queue name cannot be empty"
)

type QueueConfig struct {
	Tags                      map[string]string
	ReceiveWaitTimeSeconds    int // Set to 20 if not sure - it will enable long polling
	MaxReceiveCount           int
	DelaySeconds              int
	MaxMessageRetentionPeriod int
	DefaultVisibilityTimeout  int
}

func GetQueueURL(sqsClient ISQSClient, queueName string) (string, error) {
	ctx := context.Background()
	result, err := sqsClient.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName),
	})
	if err != nil {
		return "", err
	}
	return *result.QueueUrl, nil
}

func setupNewQueue(sqsClient ISQSClient, queueName string, config QueueConfig) (*sqs.CreateQueueOutput, error) {
	result, err := createQueue(sqsClient, queueName, config)
	if err != nil {
		return nil, err
	}
	dlqName := queueName + ErrorQueueSuffix
	dlqResult, err := createQueue(sqsClient, dlqName, config)
	if err != nil {
		return nil, err
	}
	err = setUpDeadLetterQueue(sqsClient, *result.QueueUrl, *dlqResult.QueueUrl, config.MaxReceiveCount)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func createQueue(sqsClient ISQSClient, queueName string, config QueueConfig) (*sqs.CreateQueueOutput, error) {
	ctx := context.Background()
	queueTags := createQueueTags(config.Tags)
	if queueTags == nil {
		return nil, errors.New(ErrMissingTags)
	}
	waitTime := strconv.Itoa(config.ReceiveWaitTimeSeconds)
	delaySeconds := strconv.Itoa(config.DelaySeconds)
	maxMessageRetentionPeriod := strconv.Itoa(config.MaxMessageRetentionPeriod)
	defaultVisibilityTimeout := strconv.Itoa(config.DefaultVisibilityTimeout)
	result, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
		Attributes: map[string]string{
			"ReceiveMessageWaitTimeSeconds": waitTime,
			"DelaySeconds":                  delaySeconds,
			"MessageRetentionPeriod":        maxMessageRetentionPeriod,
			"VisibilityTimeout":             defaultVisibilityTimeout,
		},
		QueueName: aws.String(queueName),
		Tags:      queueTags,
	})
	if err != nil {
		return nil, err
	}

	return result, err
}

func setUpDeadLetterQueue(sqsClient ISQSClient, queueURL, dlqURL string, maxReceiveCount int) error {
	ctx := context.Background()
	dlqArn, err := getQueueArn(sqsClient, dlqURL)
	if err != nil {
		return err
	}
	maxReceive := strconv.Itoa(maxReceiveCount)
	policy := map[string]string{
		"deadLetterTargetArn": dlqArn,
		"maxReceiveCount":     maxReceive,
	}

	b, err := json.Marshal(policy)
	if err != nil {
		return err
	}

	_, err = sqsClient.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{
		QueueUrl: aws.String(queueURL),
		Attributes: map[string]string{
			string(types.QueueAttributeNameRedrivePolicy): string(b),
		},
	})
	if err != nil {
		return err
	}

	return nil
}

func getQueueArn(sqsClient ISQSClient, queueURL string) (string, error) {
	ctx := context.Background()
	result, err := sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
		QueueUrl:       aws.String(queueURL),
	})
	if err != nil {
		return "", err
	}

	return result.Attributes["QueueArn"], nil
}

func validateQueueName(queueName string) error {
	if len(queueName) > 80 {
		return errors.New(errQueueNameTooLong)
	}

	if queueName == "" {
		return errors.New(errQueueNameEmpty)
	}

	ok, err := regexp.MatchString("^[a-zA-Z0-9-_]+$", queueName)
	if !ok {
		return errors.New(errNonAlphaNumericCharsInQueueName)
	}
	if err != nil {
		return err
	}
	return nil
}
