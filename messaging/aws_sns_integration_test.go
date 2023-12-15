package zaws

import (
	"ammyy9908/go-common-libraries/gracefulshutdown"
	"ammyy9908/go-common-libraries/logger"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
)

const (
	localstackEndpointSns = "http://localhost:4566"
	localstackRegionSns   = "ap-south-1"
	testTopic1            = "test_t1"
	snsQ1                 = "test-sns-q1"
)

var tagsSns = map[string]string{
	"serviceName":  "test-service",
	"businessUnit": "test-BU",
	"environment":  "test",
	"env":          "test",
	"serviceGroup": "test-service-group",
	"ownerEmail":   "owner-email",
}

func Test_Sns_Aws(t *testing.T) {
	t.Run("should send message to topic and be consumed by listener", func(t *testing.T) {
		testResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{
				PartitionID:   "aws",
				URL:           localstackEndpointSns,
				SigningRegion: localstackRegionSns,
			}, nil
		})

		cfg, err := awsConfig.LoadDefaultConfig(context.Background(), awsConfig.WithEndpointResolverWithOptions(testResolver), awsConfig.WithSharedConfigProfile("default"))
		assert.Nil(t, err)

		c := QueueConfig{
			Tags:                   tagsSns,
			ReceiveWaitTimeSeconds: 20,
			MaxReceiveCount:        10,
		}

		log := logger.New(logger.Debug)
		ch := make(chan struct{})
		gsm := gracefulshutdown.NewManager(log, ch)
		handler := &SimpleSnsPrint{}

		client, err := NewManagerWithConfig(cfg)
		assert.Nil(t, err)
		_, err = client.CreateQueue(snsQ1, c)
		assert.Nil(t, err)

		_, err = client.CreateTopic(testTopic1, tagsSns)
		assert.Nil(t, err)

		err = client.SubscribeQueueToTopic(snsQ1, testTopic1, true)
		assert.Nil(t, err)

		listener, err := NewListenerWithConfig(snsQ1, cfg, ListenerConfig{
			Logger:                    log,
			Handler:                   handler,
			GracefulShutdownManager:   gsm,
			ReceiveMessageWaitSeconds: 1,
			MaxNumberOfMessages:       1,
		})

		assert.Nil(t, err)

		topicPublisher, err := NewTopicPublisherWithConfig(cfg, testTopic1)
		assert.Nil(t, err)

		messageBody := "test message 1"
		err = topicPublisher.Publish(messageBody)
		assert.Nil(t, err)

		go listener.Listen()
		time.Sleep(1 * time.Second)

		assert.Equal(t, messageBody, *handler.message.Body)
	})
}

type SimpleSnsPrint struct {
	message types.Message
}

func (p *SimpleSnsPrint) Handle(message types.Message) error {
	fmt.Println("Listener message: " + *message.Body)
	p.message = message
	return nil
}
