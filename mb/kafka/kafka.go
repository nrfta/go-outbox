package kafka

import (
	"context"
	"fmt"

	"github.com/nrfta/go-outbox"
	"github.com/nrfta/go-outbox/mb"

	"github.com/twmb/franz-go/pkg/kgo"
)

type kafkaMessageBroker struct {
	client *kgo.Client
}

func New(client *kgo.Client) (outbox.MessageBroker[mb.Message], error) {
	return &kafkaMessageBroker{client}, nil
}

func (mb kafkaMessageBroker) Send(ctx context.Context, msg mb.Message) error {
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
