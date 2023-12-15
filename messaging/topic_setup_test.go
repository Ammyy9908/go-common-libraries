package zaws

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/ammyy9908/go-common-libraries/messaging/mock"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go/middleware"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestGetTopicArn(t *testing.T) {
	ctx := context.Background()
	t.Run("getTopicArn returns topic arn when given a topic name", func(t *testing.T) {
		topicName := "test-topic"
		snsClient := mock.NewMockISNSClient(gomock.NewController(t))
		snsClient.
			EXPECT().
			CreateTopic(ctx, &sns.CreateTopicInput{
				Name: aws.String(topicName),
			}).
			Return(&sns.CreateTopicOutput{TopicArn: aws.String("arn:test-topic")}, nil)

		want := "arn:test-topic"
		got, err := getTopicArn(snsClient, topicName)

		assert.Equal(t, want, got)
		assert.Nil(t, err)
	})
}

func Test_setTopicPolicy(t *testing.T) {
	ctx := context.Background()
	sqsClient := mock.NewMockISQSClient(gomock.NewController(t))
	queueURL := "www.test-topic.com"
	t.Run("setTopicPolicy sets queue policy and does not return an error when setting up policy", func(t *testing.T) {
		sqsClient.
			EXPECT().
			SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{
				Attributes: map[string]string{"Policy": "policy"},
				QueueUrl:   aws.String("www.test-topic.com"),
			}).
			Return(&sqs.SetQueueAttributesOutput{}, nil)

		err := setTopicPolicy(sqsClient, queueURL, "policy")

		assert.Nil(t, err)
	})

	t.Run("setTopicPolicy returns an error when SetQueueAttributes returns an error", func(t *testing.T) {
		sqsClient.
			EXPECT().
			SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{
				Attributes: map[string]string{"Policy": "policy"},
				QueueUrl:   aws.String("www.test-topic.com"),
			}).
			Return(nil, errors.New("set attribute error"))

		err := setTopicPolicy(sqsClient, queueURL, "policy")

		assert.NotNil(t, err)
	})
}

func Test_getPolicyContent(t *testing.T) {
	ctx := context.Background()
	sqsClient := mock.NewMockISQSClient(gomock.NewController(t))
	queueURL := "www.test-topic.com"
	t.Run("getPolicyContent returns a formatted policy given queue and subscription arn", func(t *testing.T) {
		sqsClient.
			EXPECT().
			GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
				AttributeNames: []types.QueueAttributeName{types.QueueAttributeNamePolicy},
				QueueUrl:       aws.String(queueURL),
			}).
			Return(&sqs.GetQueueAttributesOutput{
				Attributes:     map[string]string{"Policy": "{\"Version\":\"2012-10-17\",\"Id\":\"arn:test-queue/SQSDefaultPolicy\"}"},
				ResultMetadata: middleware.Metadata{},
			}, nil)

		sqsClient.
			EXPECT().
			GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
				AttributeNames: []types.QueueAttributeName{types.QueueAttributeNamePolicy},
				QueueUrl:       aws.String(queueURL),
			}).
			Return(&sqs.GetQueueAttributesOutput{
				Attributes:     map[string]string{"Policy": "{\"Version\":\"2012-10-17\",\"Id\":\"arn:test-queue/SQSDefaultPolicy\",\"Statement\":[{\"Sid\":\"Sid1580665629194\",\"Effect\":\"Allow\",\"Principal\":{\"Service\":\"*\"},\"Action\":\"SQS:SendMessage\",\"Resource\":\"www.test-topic.com\",\"Condition\":{\"ArnEquals\":{\"aws:SourceArn\":\"arn:aws:sns:ap-south-1:00000000:test-topic0\"}}}]}"},
				ResultMetadata: middleware.Metadata{},
			}, nil)

		sqsClient.
			EXPECT().
			GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
				AttributeNames: []types.QueueAttributeName{types.QueueAttributeNamePolicy},
				QueueUrl:       aws.String(queueURL),
			}).
			Return(&sqs.GetQueueAttributesOutput{
				Attributes:     map[string]string{"Policy": "{\"Version\":\"2012-10-17\",\"Id\":\"arn:test-queue/SQSDefaultPolicy\",\"Statement\":[{\"Sid\":\"Sid1580665629194\",\"Effect\":\"Allow\",\"Principal\":{\"Service\":\"*\"},\"Action\":\"SQS:SendMessage\",\"Resource\":\"www.test-topic.com\",\"Condition\":{\"ArnEquals\":{\"aws:SourceArn\":\"arn:aws:sns:ap-south-1:00000000:test-topic0\"}}},{\"Sid\":\"Sid1580665629194\",\"Effect\":\"Allow\",\"Principal\":{\"Service\":\"*\"},\"Action\":\"SQS:SendMessage\",\"Resource\":\"www.test-topic.com\",\"Condition\":{\"ArnEquals\":{\"aws:SourceArn\":\"arn:aws:sns:ap-south-1:00000000:test-topic1\"}}}]}"},
				ResultMetadata: middleware.Metadata{},
			}, nil)

		subArn0 := "arn:aws:sns:ap-south-1:00000000:test-topic0"
		subArn1 := "arn:aws:sns:ap-south-1:00000000:test-topic1"
		subArn2 := "arn:aws:sns:ap-south-1:00000000:test-topic2"
		want := PolicyStruct{
			Version: "2012-10-17",
			ID:      "arn:test-queue/SQSDefaultPolicy",
			Statement: []StatementStruct{{
				Sid:    "Sid1580665629194",
				Effect: "Allow",
				Principal: PrincipalStruct{
					Service: "*",
				},
				Action:   "SQS:SendMessage",
				Resource: queueURL,
				Condition: ConditionStruct{
					ArnEqualsStruct{
						subArn0,
					},
				},
			}, {
				Sid:    "Sid1580665629194",
				Effect: "Allow",
				Principal: PrincipalStruct{
					Service: "*",
				},
				Action:   "SQS:SendMessage",
				Resource: queueURL,
				Condition: ConditionStruct{
					ArnEqualsStruct{
						subArn1,
					},
				},
			}, {
				Sid:    "Sid1580665629194",
				Effect: "Allow",
				Principal: PrincipalStruct{
					Service: "*",
				},
				Action:   "SQS:SendMessage",
				Resource: queueURL,
				Condition: ConditionStruct{
					ArnEqualsStruct{
						subArn2,
					},
				},
			},
			},
		}

		subArnArr := []string{subArn0, subArn1, subArn2}
		var parsedContent PolicyStruct
		got := func() string {
			var newPolicy string
			for _, subArn := range subArnArr {
				content, err := getPolicyContent(sqsClient, queueURL, subArn)
				assert.Nil(t, err)
				err = json.Unmarshal([]byte(content), &parsedContent)
				assert.Nil(t, err)
				assert.Equal(t, subArn, parsedContent.Statement[len(parsedContent.Statement)-1].Condition.ArnEquals.AwsSourceArn)
				newPolicy = content
			}
			return newPolicy
		}

		var gotPolicyStruct PolicyStruct
		err := json.Unmarshal([]byte(got()), &gotPolicyStruct)
		assert.Nil(t, err)
		assert.Equal(t, want, gotPolicyStruct)
	})
}
