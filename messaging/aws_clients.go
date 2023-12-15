package zaws

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type ISQSClient interface {
	GetQueueUrl(ctx context.Context, params *sqs.GetQueueUrlInput, options ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error)
	CreateQueue(ctx context.Context, params *sqs.CreateQueueInput, options ...func(*sqs.Options)) (*sqs.CreateQueueOutput, error)
	GetQueueAttributes(ctx context.Context, params *sqs.GetQueueAttributesInput, options ...func(*sqs.Options)) (*sqs.GetQueueAttributesOutput, error)
	SetQueueAttributes(ctx context.Context, params *sqs.SetQueueAttributesInput, options ...func(*sqs.Options)) (*sqs.SetQueueAttributesOutput, error)
	SendMessage(ctx context.Context, params *sqs.SendMessageInput, options ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
	ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, options ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, options ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
}

type ISNSClient interface {
	CreateTopic(ctx context.Context, params *sns.CreateTopicInput, options ...func(*sns.Options)) (*sns.CreateTopicOutput, error)
	Subscribe(ctx context.Context, params *sns.SubscribeInput, options ...func(*sns.Options)) (*sns.SubscribeOutput, error)
	Publish(ctx context.Context, params *sns.PublishInput, options ...func(*sns.Options)) (*sns.PublishOutput, error)
}

type MessageHandler interface {
	Handle(message types.Message) error
}
