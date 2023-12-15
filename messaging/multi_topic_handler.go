package zaws

import (
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/goccy/go-json"
)

type Event struct {
	Subject string `json:"subject"`
	Message string `json:"message"`
}

type IMultiTopicHandler interface {
	RegisterHandler(subject string, handlerFunc EventHandler)
	Handle(message types.Message) error
}

type MultiTopicHandler struct {
	Handlers map[string]EventHandler
}

type EventHandler func(message string) error

func NewMultiTopicHandler() *MultiTopicHandler {
	h := make(map[string]EventHandler)
	return &MultiTopicHandler{Handlers: h}
}

func (h *MultiTopicHandler) RegisterHandler(subject string, handlerFunc EventHandler) {
	h.Handlers[subject] = handlerFunc
}

func (h *MultiTopicHandler) Handle(message types.Message) error {
	var event Event
	b := []byte(*message.Body)
	fmt.Println(string(b))
	err := json.Unmarshal(b, &event)
	if err != nil {
		fmt.Println(message.Body)
		return err
	}
	handler, ok := h.Handlers[event.Subject]
	if !ok {
		return fmt.Errorf("no handler for Subject: %s", event.Subject)
	}

	err = handler(event.Message)
	return err
}
