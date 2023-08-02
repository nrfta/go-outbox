package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/nrfta/go-outbox"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Message struct {
	Key       []byte
	Value     []byte
	Headers   []kgo.RecordHeader
	Timestamp time.Time
	Topic     string
}

type kafkaMessageBroker struct {
	client *kgo.Client
}

func New(client *kgo.Client) (outbox.MessageBroker[Message], error) {
	return &kafkaMessageBroker{client}, nil
}

func (mb kafkaMessageBroker) Send(ctx context.Context, msg Message) error {
	record := &kgo.Record{
		Key:       msg.Key,
		Value:     msg.Value,
		Headers:   msg.Headers,
		Timestamp: msg.Timestamp,
		Topic:     msg.Topic,
	}
	if err := mb.client.ProduceSync(ctx, record).FirstErr(); err != nil {
		return fmt.Errorf("record had a produce error while synchronously producing: %v", err)
	}

	return nil
}
