package zaws

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
)

func TestMultiTopicHandler_RegisterHandler(t *testing.T) {
	t.Run("RegisterHandler adds a function into the MultiTopic handlers handler map", func(t *testing.T) {
		mtH := NewMultiTopicHandler()

		testSubject := "test subject"
		testHandler := EventHandler(func(message string) error { return nil })
		mtH.RegisterHandler(testSubject, testHandler)

		handler, ok := mtH.Handlers[testSubject]

		assert.True(t, ok)
		assert.Equal(t, testHandler("test"), handler("test"))
	})
}

func TestMultiTopicHandler_Handle(t *testing.T) {
	mtH := NewMultiTopicHandler()
	testSubject := "test-topic"
	testHandler := EventHandler(func(message string) error { return nil })
	mtH.RegisterHandler(testSubject, testHandler)

	t.Run("MultiTopicHandler Handle takes in a message, fetches the correct handler and handles the message", func(t *testing.T) {
		message := types.Message{Body: aws.String(`{"subject":"test-topic","message":"test message"}`)}

		err := mtH.Handle(message)

		assert.Nil(t, err)
	})

	t.Run("MultiTopicHandler Handle takes in a message, returns an error if the subject handler does not exist", func(t *testing.T) {
		message := types.Message{Body: aws.String(`{"subject":"nonexistent","message":"test message"}`)}

		err := mtH.Handle(message)

		assert.NotNil(t, err)
	})

	t.Run("MultiTopicHandler Handle takes in a message, fetches returns an error if the message does not have correct format", func(t *testing.T) {
		message := types.Message{Body: aws.String("bad message format")}

		err := mtH.Handle(message)

		assert.NotNil(t, err)
	})

	t.Run("MultiTopicHandler Handle takes in a message, returns an error if the message handler returns an error", func(t *testing.T) {
		errorHandler := EventHandler(func(message string) error { return fmt.Errorf("test error") })
		mtH.RegisterHandler("error", errorHandler)

		message := types.Message{Body: aws.String(`{"subject":"error","message":"test message"}`)}

		err := mtH.Handle(message)

		assert.NotNil(t, err)
	})
}
