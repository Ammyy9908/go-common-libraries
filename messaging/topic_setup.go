package zaws

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/goccy/go-json"
)

type PolicyStruct struct {
	Version   string            `json:"Version"`
	ID        string            `json:"Id"`
	Statement []StatementStruct `json:"Statement"`
}

type StatementStruct struct {
	Sid       string          `json:"Sid"`
	Effect    string          `json:"Effect"`
	Principal PrincipalStruct `json:"Principal"`
	Action    string          `json:"Action"`
	Resource  string          `json:"Resource"`
	Condition ConditionStruct `json:"Condition"`
}

type PrincipalStruct struct {
	Service string `json:"Service"`
}

type ConditionStruct struct {
	ArnEquals ArnEqualsStruct `json:"ArnEquals"`
}

type ArnEqualsStruct struct {
	AwsSourceArn string `json:"aws:SourceArn"`
}

func getTopicArn(snsClient ISNSClient, topicName string) (string, error) {
	ctx := context.Background()
	// No method for just getting topic ARN
	// Create topic is idempotent, will return result if the topic exists
	result, err := snsClient.CreateTopic(ctx, &sns.CreateTopicInput{
		Name: aws.String(topicName),
	})
	if err != nil {
		return "", err
	}

	return *result.TopicArn, nil
}

func setTopicPolicy(sqsClient ISQSClient, queueURL, policy string) error {
	ctx := context.Background()
	attributes := make(map[string]string, 1)
	attributes["Policy"] = policy

	setQueueAttrInput := &sqs.SetQueueAttributesInput{
		QueueUrl:   aws.String(queueURL),
		Attributes: attributes,
	}

	_, err := sqsClient.SetQueueAttributes(ctx, setQueueAttrInput)
	if err != nil {
		return err
	}
	return nil
}

func getPolicyContent(sqsClient ISQSClient, queueURL string, topicARN string) (string, error) {
	ctx := context.Background()
	queueArn, _ := getQueueArn(sqsClient, queueURL)
	queueAttrs, err := sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNamePolicy},
		QueueUrl:       aws.String(queueURL),
	})
	if err != nil {
		return "", err
	}

	if _, ok := queueAttrs.Attributes[string(types.QueueAttributeNamePolicy)]; !ok {
		queueAttrs.Attributes = map[string]string{"Policy": "{\"Version\":\"2012-10-17\",\"Id\":\"arn:test-queue/SQSDefaultPolicy\"}"}
	}

	newPolicyWithTopic := &PolicyStruct{}
	err = json.Unmarshal([]byte(queueAttrs.Attributes[string(types.QueueAttributeNamePolicy)]), &newPolicyWithTopic)
	if err != nil {
		return "", err
	}

	newPolicyWithTopic.Statement = append(newPolicyWithTopic.Statement, StatementStruct{
		Sid:    "Sid1580665629194",
		Effect: "Allow",
		Principal: PrincipalStruct{
			Service: "sns.amazonaws.com",
		},
		Action:   "SQS:SendMessage",
		Resource: queueArn,
		Condition: ConditionStruct{
			ArnEqualsStruct{
				AwsSourceArn: topicARN,
			},
		},
	})

	newPolicyTemplateBytes, err := json.Marshal(newPolicyWithTopic)
	jsonString := string(newPolicyTemplateBytes)

	fmt.Println(jsonString)
	if err != nil {
		return "", err
	}
	return string(newPolicyTemplateBytes), nil
}
