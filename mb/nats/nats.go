package mbNats

import (
	"context"
	"fmt"

	"github.com/nrfta/go-outbox"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type natsJetstreamBroker struct {
	js jetstream.JetStream
}

func NewJetstream(js jetstream.JetStream) (outbox.MessageBroker[*nats.Msg], error) {
	return &natsJetstreamBroker{js}, nil
}

func (b natsJetstreamBroker) Send(ctx context.Context, msg *nats.Msg) error {
	_, err := b.js.PublishMsg(ctx, msg)
	if err != nil {
		return fmt.Errorf("nats jetstream publish message: %v", err)
	}

	return nil
}

type natsBroker struct {
	client *nats.Conn
}

func New(client *nats.Conn) (outbox.MessageBroker[*nats.Msg], error) {
	return &natsBroker{client}, nil
}

func (b natsBroker) Send(ctx context.Context, msg *nats.Msg) error {
	err := b.client.PublishMsg(msg)
	if err != nil {
		return fmt.Errorf("nats publish message: %v", err)
	}

	return nil
}
