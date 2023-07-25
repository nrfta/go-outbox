package mb

import (
	"context"
	"fmt"
	"go-outbox"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

type kafkaMessageBroker struct {
	client *kgo.Client
}

var _ outbox.MessageBroker = &kafkaMessageBroker{}

func New(client *kgo.Client) (*kafkaMessageBroker, error) {
	return &kafkaMessageBroker{client}, nil
}

func (kafkaMessageBroker) EncodeMessage(msg any) ([]byte, error) {
	protoMsg, ok := msg.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("invalid message type: %T", msg)
	}

	return proto.Marshal(protoMsg)
}

func (mb kafkaMessageBroker) Send(ctx context.Context, msg []byte) error {
	record := &kgo.Record{Topic: "foo", Value: msg}

	if err := mb.client.ProduceSync(ctx, record).FirstErr(); err != nil {
		return fmt.Errorf("record had a produce error while synchronously producing: %v", err)
	}

	return nil
}
