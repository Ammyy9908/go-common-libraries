package zaws

import (
	"context"
	"testing"

	"github.com/ammyy9908/go-common-libraries/gracefulshutdown"
	"github.com/ammyy9908/go-common-libraries/logger"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

const (
	localstackEndpoint = "http://localhost:4566"
	localstackRegion   = "ap-south-1"
	initQ1             = "init_test_q1"
	testQ1             = "test_q1"
	testQ2             = "test_q2"
	testQ3             = "test_q3"
	testQ4             = "test_q4"
)

var tags = map[string]string{
	"serviceName":  "test-service",
	"businessUnit": "test-BU",
	"environment":  "test",
	"env":          "test",
	"serviceGroup": "test-service-group",
	"ownerEmail":   "owner-email",
}

func Test_Aws_Sqs_Without_Listener(t *testing.T) {
	ctx := context.Background()
	t.Run("should send message to queue and be consumed without listener", func(t *testing.T) {

		testResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{
				PartitionID:   "aws",
				URL:           localstackEndpoint,
				SigningRegion: localstackRegion,
			}, nil
		})

		cfg, err := awsConfig.LoadDefaultConfig(context.Background(), awsConfig.WithEndpointResolverWithOptions(testResolver), awsConfig.WithSharedConfigProfile("default"))

		c := QueueConfig{
			Tags:                   tags,
			ReceiveWaitTimeSeconds: 20,
			MaxReceiveCount:        10,
		}

		client, err := NewManagerWithConfig(cfg)
		assert.Nil(t, err)
		queue, err := client.CreateQueue(testQ1, c)
		assert.Nil(t, err)

		queueURL := *queue.QueueUrl

		receiveMessage := &sqs.ReceiveMessageInput{
			MaxNumberOfMessages: 1,
			QueueUrl:            aws.String(queueURL),
			VisibilityTimeout:   10,
			WaitTimeSeconds:     0,
		}

		message, err := client.sqsClient.SendMessage(ctx, &sqs.SendMessageInput{
			MessageBody: aws.String("test message 1"),
			QueueUrl:    aws.String(queueURL),
		})
		assert.Nil(t, err)
		output, err := client.sqsClient.ReceiveMessage(ctx, receiveMessage)
		assert.Nil(t, err)
		assert.NotEmpty(t, output.Messages)

		err = deleteMessages(client, []string{queueURL}, output)
		assert.Nil(t, err)
		assert.Equal(t, *message.MessageId, *output.Messages[0].MessageId)
	})
}

func Test_Aws_Sqs_With_Listener(t *testing.T) {
	t.Run("should send message to queue and be consumed by listener", func(t *testing.T) {
		testResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{
				PartitionID:   "aws",
				URL:           localstackEndpoint,
				SigningRegion: localstackRegion,
			}, nil
		})

		cfg, err := awsConfig.LoadDefaultConfig(context.Background(), awsConfig.WithEndpointResolverWithOptions(testResolver), awsConfig.WithSharedConfigProfile("default"))

		c := QueueConfig{
			Tags:                   tags,
			ReceiveWaitTimeSeconds: 20,
			MaxReceiveCount:        10,
		}

		log := logger.New(logger.Debug)
		ch := make(chan struct{})
		channel := make(chan bool)
		gsm := gracefulshutdown.NewManager(log, ch)
		handler := &SimpleSqsPrint{channel: channel}

		client, err := NewManagerWithConfig(cfg)
		assert.Nil(t, err)
		queue, err := client.CreateQueue(testQ4, c)
		assert.Nil(t, err)
		queueURL := *queue.QueueUrl
		listener := createTestListenerForSqs(queueURL, log, gsm, handler, client.sqsClient, testQ4)

		message := sendMessageToQueue(t, client, "test-message", queueURL)

		go listener.Listen()

		<-channel
		gsm.ShutdownChannel <- 0

		assert.Equal(t, message, *handler.message.Body)
	})
}

func Test_Aws_Sqs_With_Multiple_Queues(t *testing.T) {
	t.Run("should send message to multiple queues and be consumed by multiple listeners", func(t *testing.T) {

		testResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{
				PartitionID:   "aws",
				URL:           localstackEndpoint,
				SigningRegion: localstackRegion,
			}, nil
		})

		cfg, err := awsConfig.LoadDefaultConfig(context.Background(), awsConfig.WithEndpointResolverWithOptions(testResolver), awsConfig.WithSharedConfigProfile("default"))

		c := QueueConfig{
			Tags:                   tags,
			ReceiveWaitTimeSeconds: 20,
			MaxReceiveCount:        10,
		}

		log := logger.New(logger.Debug)
		ch := make(chan struct{})
		channel := make(chan bool)
		channel2 := make(chan bool)
		channel3 := make(chan bool)

		gsm := gracefulshutdown.NewManager(log, ch)
		handler := &SimpleSqsPrint{channel: channel}
		handler2 := &SimpleSqsPrint{channel: channel2}
		handler3 := &SimpleSqsPrint{channel: channel3}

		client, err := NewManagerWithConfig(cfg)
		assert.Nil(t, err)

		//created at startup of localstack container from scripts, localstack_bootstrap/sqs_bootstrap.sh
		queueURL, _ := GetQueueURL(client.sqsClient, initQ1)

		queue2, err := client.CreateQueue(testQ2, c)
		if err != nil {
			t.Fatalf(err.Error())
		}

		queue3, err := client.CreateQueue(testQ3, c)
		if err != nil {
			t.Fatalf(err.Error())
		}

		queueURL2 := *queue2.QueueUrl
		queueURL3 := *queue3.QueueUrl

		listener := *createTestListenerForSqs(queueURL, log, gsm, handler, client.sqsClient, initQ1)
		listener2 := *createTestListenerForSqs(queueURL2, log, gsm, handler2, client.sqsClient, testQ2)
		listener3 := *createTestListenerForSqs(queueURL3, log, gsm, handler3, client.sqsClient, testQ3)

		message := sendMessageToQueue(t, client, "test-message", queueURL)
		message2 := sendMessageToQueue(t, client, "test-message2", queueURL2)
		message3 := sendMessageToQueue(t, client, "test-message3", queueURL3)

		go listener3.Listen()
		go listener2.Listen()
		go listener.Listen()

		<-channel
		<-channel2
		<-channel3
		gsm.ShutdownChannel <- 0

		assert.Equal(t, message, *handler.message.Body)
		assert.Equal(t, message2, *handler2.message.Body)
		assert.Equal(t, message3, *handler3.message.Body)
	})
}

func sendMessageToQueue(t *testing.T, client *Manager, messageBody, queueURL string) string {
	ctx := context.Background()
	_, err := client.sqsClient.SendMessage(ctx, &sqs.SendMessageInput{
		MessageBody: aws.String(messageBody),
		QueueUrl:    aws.String(queueURL),
	})
	if err != nil {
		t.Fatalf("Unable to send messages to queue(s)")
	}

	return messageBody
}

func createTestListenerForSqs(sqsURL string, log *zap.SugaredLogger, gsm *gracefulshutdown.Manager, handler MessageHandler, sqsClient ISQSClient, queueName string) *SQSListener {
	return &SQSListener{
		queueName:                 queueName,
		queueURL:                  aws.String(sqsURL),
		sqsClient:                 sqsClient,
		logger:                    log,
		handler:                   handler,
		gracefulShutdownManager:   gsm,
		receiveMessageWaitSeconds: 1,
		maxNumberOfMessages:       1,
	}
}

type SimpleSqsPrint struct {
	message types.Message
	channel chan bool
}

func (p *SimpleSqsPrint) Handle(message types.Message) error {
	p.message = message
	p.channel <- true
	return nil
}

func deleteMessages(client *Manager, queueUrls []string, output *sqs.ReceiveMessageOutput) error {
	ctx := context.Background()
	for _, queueURL := range queueUrls {
		_, err := client.sqsClient.DeleteMessage(ctx, &sqs.DeleteMessageInput{
			QueueUrl:      aws.String(queueURL),
			ReceiptHandle: output.Messages[0].ReceiptHandle,
		})
		if err != nil {
			return err
		}
	}
	return nil
}
