package outbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/rs/xid"
)

var (
	ErrRecordNotFound = errors.New("record not found")
)

type MessageBroker interface {
	EncodeMessage(msg any) ([]byte, error)
	Send(context.Context, []byte) error
}

type Record struct {
	ID   xid.ID
	Data []byte
}

type Store interface {
	// CreateRecordTx stores the Record within the provided Tx.
	CreateRecordTx(context.Context, *sql.Tx, Record) (*Record, error)

	// Listen creates a channel of record IDs to process.
	Listen() <-chan xid.ID

	// GetForUpdate finds a Record in the Store by the provided ID. This method
	// must lock the returned Record while being processed to ensure
	// concurrent integrity.
	GetForUpdate(context.Context, xid.ID) (*Record, error)

	// Delete removes a record from the Store by the provided ID.
	Delete(context.Context, xid.ID) error

	// ProcessTx performs the function provided inside a transaction.
	ProcessTx(ctx context.Context, fn func(Store) bool) error
}

type Logger interface {
	Log(...any)
}

type Outbox interface {
	// SendTx stores the provided message within the provided Tx
	SendTx(ctx context.Context, tx *sql.Tx, msg any) error
}

type outbox struct {
	store Store
	mb    MessageBroker

	logger Logger

	numRoutines int
}

type option func(o *outbox)

func WithNumberOfRoutines(n int) option {
	return func(o *outbox) {
		o.numRoutines = n
	}
}

func New(s Store, mb MessageBroker, opts ...option) Outbox {
	ob := &outbox{
		store:       s,
		mb:          mb,
		numRoutines: 5,
	}

	for _, o := range opts {
		o(ob)
	}

	go ob.dispatch()

	return ob
}

func (o outbox) SendTx(ctx context.Context, tx *sql.Tx, msg any) error {
	raw, err := o.mb.EncodeMessage(msg)
	if err != nil {
		return err
	}

	if _, err := o.store.CreateRecordTx(ctx, tx, Record{Data: raw}); err != nil {
		return err
	}

	return nil
}

func (o outbox) dispatch() {
	tokens := make(chan struct{}, o.numRoutines)
	for id := range o.store.Listen() {
		tokens <- struct{}{}
		go func(id xid.ID) {
			if err := o.process(id); err != nil {
				o.logger.Log("err", err)
			}

			<-tokens
		}(id)
	}
}

func (o outbox) process(id xid.ID) (outErr error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	defer cancel()

	if err := o.store.ProcessTx(ctx, o.processMessageTx(ctx, id)); err != nil {
		return fmt.Errorf("unable to process transaction for record %s: %v", id, err)
	} else if outErr != nil {
		return
	}

	o.logger.Log("msg", fmt.Sprintf("outbox record %s processed successfully", id))
	return
}

func (o outbox) processMessageTx(ctx context.Context, id xid.ID) func(s Store) bool {
	return func(s Store) bool {
		record, err := s.GetForUpdate(ctx, id)
		if errors.Is(err, ErrRecordNotFound) {
			return true
		} else if err != nil {
			o.logger.Log("err", err)
			return false
		}

		if err := o.mb.Send(ctx, record.Data); err != nil {
			o.logger.Log("err", err)
			return false
		}

		if err := s.Delete(ctx, record.ID); err != nil {
			o.logger.Log("err", err)
			return false
		}

		return true
	}
}
