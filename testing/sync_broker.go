package testing

import (
	"context"

	"github.com/nats-io/nats.go"
	"github.com/nrfta/go-outbox"
)

// SyncBroker implements outbox.MessageBroker[*nats.Msg] for synchronous testing.
// It receives messages from the outbox and immediately routes them to consumer
// handlers via SyncJetStream, enabling deterministic event processing in tests.
type SyncBroker struct {
	syncJS *SyncJetStream
}

// NewSyncBroker creates a synchronous message broker for testing
func NewSyncBroker(syncJS *SyncJetStream) *SyncBroker {
	return &SyncBroker{
		syncJS: syncJS,
	}
}

// Send implements outbox.MessageBroker[*nats.Msg]
// Instead of publishing to NATS, it immediately delivers to the sync JetStream
func (b *SyncBroker) Send(ctx context.Context, msg *nats.Msg) error {
	// Use the sync JetStream to route and handle the message synchronously
	return b.syncJS.SendMessage(ctx, msg.Subject, msg.Data)
}

// Ensure SyncBroker implements outbox.MessageBroker[*nats.Msg]
var _ outbox.MessageBroker[*nats.Msg] = (*SyncBroker)(nil)
