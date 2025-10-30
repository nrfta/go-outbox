package testing

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/gob"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/nrfta/go-outbox"
	"github.com/rs/xid"
)

// SyncStore is a store for testing that immediately sends messages
// instead of storing them for async processing. This enables synchronous
// event processing in tests without the need for background workers.
type SyncStore struct {
	broker outbox.MessageBroker[*nats.Msg]
}

// NewSyncStore creates a new synchronous outbox store for testing
func NewSyncStore(broker outbox.MessageBroker[*nats.Msg]) *SyncStore {
	return &SyncStore{
		broker: broker,
	}
}

func (s *SyncStore) CreateRecordTx(ctx context.Context, tx *sql.Tx, record outbox.Record) (*outbox.Record, error) {
	// Instead of storing, immediately send the message
	if s.broker != nil && len(record.Message) > 0 {
		// Decode the gob-encoded NATS message
		var msg nats.Msg
		decoder := gob.NewDecoder(bytes.NewReader(record.Message))
		if err := decoder.Decode(&msg); err != nil {
			return nil, err
		}

		if err := s.broker.Send(ctx, &msg); err != nil {
			return nil, err
		}
	}
	return &record, nil
}

func (s *SyncStore) Listen() <-chan xid.ID {
	// Return empty channel since we're not listening
	ch := make(chan xid.ID)
	close(ch)
	return ch
}

func (s *SyncStore) GetWithLock(ctx context.Context, id xid.ID) (*outbox.Record, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *SyncStore) Delete(ctx context.Context, id xid.ID) error {
	return nil
}

func (s *SyncStore) ProcessTx(ctx context.Context, fn func(outbox.Store) bool) error {
	fn(s)
	return nil
}

func (s *SyncStore) Update(ctx context.Context, record *outbox.Record) error {
	return nil
}

// Ensure SyncStore implements outbox.Store
var _ outbox.Store = (*SyncStore)(nil)
