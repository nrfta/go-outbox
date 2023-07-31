package mb

import (
	"context"
	"fmt"
	"go-outbox"

	"github.com/twmb/franz-go/pkg/kgo"
)

type kafkaMessageBroker struct {
	client *kgo.Client
}

var _ outbox.MessageBroker[*kgo.Record] = &kafkaMessageBroker{}

func New(client *kgo.Client) (*kafkaMessageBroker, error) {
	return &kafkaMessageBroker{client}, nil
}

func (mb kafkaMessageBroker) Send(ctx context.Context, msg *kgo.Record) error {
	if err := mb.client.ProduceSync(ctx, msg).FirstErr(); err != nil {
		return fmt.Errorf("record had a produce error while synchronously producing: %v", err)
	}

	return nil
}
