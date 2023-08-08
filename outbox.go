package outbox

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/gob"
	"errors"
	"fmt"
	"os"
	"reflect"
	"time"

	"github.com/go-kit/log"
	"github.com/rs/xid"
)

var (
	errInvalidType    = errors.New("invalid message type")
	ErrRecordNotFound = errors.New("record not found")
)

type Logger log.Logger

type MessageBroker[T any] interface {
	Send(context.Context, T) error
}

type DeadLetterQueue interface {
	Send(context.Context, any, error) error
}

type Record struct {
	ID               xid.ID
	Message          []byte
	CreatedAt        time.Time
	NumberOfAttempts int
	LastAttemptAt    *time.Time
}

type Store interface {
	// CreateRecordTx stores the Record within the provided Tx.
	CreateRecordTx(context.Context, *sql.Tx, Record) (*Record, error)

	// Listen creates a channel of record IDs to process.
	Listen() <-chan xid.ID

	// GetWithLock finds a Record in the Store by the provided ID. This method
	// must lock the returned Record while being processed to ensure
	// concurrent integrity.
	GetWithLock(context.Context, xid.ID) (*Record, error)

	// Delete removes a record from the Store by the provided ID.
	Delete(context.Context, xid.ID) error

	// ProcessTx performs the function provided inside a transaction.
	ProcessTx(context.Context, func(Store) bool) error

	// Update sends the updated Record to the store
	Update(context.Context, *Record) error
}

type EncodeDecoder[T any] interface {
	Encode(data T) ([]byte, error)
	Decode(raw []byte) (T, error)
}

// Outbox implements the outbox pattern.
type Outbox interface {
	// SendTx stores the provided message within the provided Tx
	SendTx(ctx context.Context, tx *sql.Tx, msg any) error
}

type outbox[T any] struct {
	store Store
	mb    MessageBroker[T]
	dlq   DeadLetterQueue
	ed    EncodeDecoder[T]

	logger Logger

	numRoutines int
	maxRetries  int
}

type option[T any] func(o *outbox[T])

func WithNumberOfRoutines[T any](n int) option[T] {
	return func(o *outbox[T]) {
		o.numRoutines = n
	}
}

func WithMaxRetries[T any](n int) option[T] {
	return func(o *outbox[T]) {
		o.maxRetries = n
	}
}

func WithLogger[T any](logger Logger) option[T] {
	return func(o *outbox[T]) {
		o.logger = logger
	}
}

func New[T any](s Store, mb MessageBroker[T], dlq DeadLetterQueue, opts ...option[T]) Outbox {
	logger := log.NewJSONLogger(log.NewSyncWriter(os.Stderr))

	ob := &outbox[T]{
		store:       s,
		mb:          mb,
		dlq:         dlq,
		ed:          gobEncodeDecoder[T]{},
		numRoutines: 5,
		maxRetries:  10,
		logger:      logger,
	}

	for _, o := range opts {
		o(ob)
	}

	ob.logger = log.With(
		ob.logger,
		"component", "outbox",
		"messageBroker", reflect.TypeOf(mb),
	)

	go ob.dispatch()

	return ob
}

func (o outbox[T]) SendTx(ctx context.Context, tx *sql.Tx, msg any) error {
	want, ok := msg.(T)
	if !ok {
		return fmt.Errorf("%w: wanted %T, got %T", errInvalidType, want, msg)
	}

	raw, err := o.ed.Encode(want)
	if err != nil {
		return fmt.Errorf("unable to encode message: %v", err)
	}

	if _, err := o.store.CreateRecordTx(ctx, tx, Record{Message: raw}); err != nil {
		return fmt.Errorf("unable to create record in transaction: %v", err)
	}

	return nil
}

func (o outbox[T]) dispatch() {
	tokens := make(chan struct{}, o.numRoutines)
	for id := range o.store.Listen() {
		tokens <- struct{}{}
		go func(id xid.ID) {
			o.process(id)
			<-tokens
		}(id)
	}
}

func (o outbox[T]) process(id xid.ID) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := o.store.ProcessTx(ctx, o.processMessageTx(ctx, id)); err != nil {
		o.logger.Log("err", fmt.Errorf("process transaction error: %v", err))
	}
}

func (o outbox[T]) processMessageTx(ctx context.Context, id xid.ID) func(s Store) bool {
	return func(s Store) (success bool) {
		var (
			err    error
			msg    T
			record *Record
		)

		defer func() {
			logger := log.With(o.logger, "recordId", id)

			if success {
				logger.Log("msg", "successfully processed message")
				return
			}

			logger.Log("err", fmt.Errorf("unable to process record: %v", err))
			if record == nil {
				return
			}

			success = true

			if err := s.Update(ctx, record); err != nil {
				logger.Log("err", fmt.Errorf("unable to update record: %v", err))
				return
			}

			if record.NumberOfAttempts < o.maxRetries {
				return
			}

			logger.Log("msg", "max retries hit, sending to DLQ")
			if err := o.dlq.Send(ctx, msg, err); err != nil {
				logger.Log("err", fmt.Errorf("unable to send record to DLQ: %v", err))
				return
			}

			if err := s.Delete(ctx, record.ID); err != nil {
				logger.Log("err", fmt.Errorf("unable to delete record after DLQ send: %v", err))
			}
		}()

		record, err = s.GetWithLock(ctx, id)
		if errors.Is(err, ErrRecordNotFound) {
			return true
		} else if err != nil {
			return false
		}

		if record.LastAttemptAt != nil && time.Since(*record.LastAttemptAt) < 90*time.Second {
			return true
		}

		msg, err = o.ed.Decode(record.Message)
		if err != nil {
			return false
		}

		mbCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()

		if err = o.mb.Send(mbCtx, msg); err != nil {
			attemptedAt := time.Now()
			record.LastAttemptAt = &attemptedAt
			record.NumberOfAttempts++
			return false
		}

		err = s.Delete(ctx, record.ID)
		return err == nil
	}
}

type gobEncodeDecoder[T any] struct{}

func (g gobEncodeDecoder[T]) Encode(data T) ([]byte, error) {
	var b bytes.Buffer
	if err := gob.NewEncoder(&b).Encode(data); err != nil {
		return nil, err
	}

	return b.Bytes(), nil
}

func (g gobEncodeDecoder[T]) Decode(raw []byte) (T, error) {
	var (
		res T
		r   = bytes.NewReader(raw)
	)
	if err := gob.NewDecoder(r).Decode(&res); err != nil {
		return *new(T), err
	}

	return res, nil
}
