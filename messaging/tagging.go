package zaws

import (
	"errors"
	"sort"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"
)

const (
	Env          string = "env"
	ServiceName  string = "serviceName"
	ServiceGroup string = "serviceGroup"
	BusinessUnit string = "businessUnit"
	OwnerEmail   string = "ownerEmail"
)

func createQueueTags(tags map[string]string) map[string]string {
	err := validateTags(tags)
	if err != nil {
		return nil
	}
	return tags
}

func createTopicTags(tags map[string]string) []types.Tag {
	err := validateTags(tags)
	if err != nil {
		return nil
	}
	var topicTags []types.Tag
	for k, v := range tags {
		topicTags = append(topicTags, types.Tag{
			Key:   aws.String(k),
			Value: aws.String(v),
		})
	}

	sort.SliceStable(topicTags, func(i, j int) bool {
		return *topicTags[i].Key < *topicTags[j].Key
	})

	return topicTags
}

func validateTags(tags map[string]string) error {
	if len(tags) == 0 {
		return errors.New(ErrMissingTags)
	}

	arrayOfRequiredTags := []string{
		Env,
		ServiceName,
		ServiceGroup,
		BusinessUnit,
		OwnerEmail,
	}

	for _, v := range arrayOfRequiredTags {
		_, ok := tags[v]
		if !ok {
			return errors.New("Missing required tag: " + v)
		}
	}

	return nil
}
