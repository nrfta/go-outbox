package outbox

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/gob"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"reflect"
	"time"

	"github.com/go-kit/log"
	"github.com/rs/xid"
)

var (
	errInvalidType    = errors.New("invalid message type")
	ErrRecordNotFound = errors.New("record not found")
	errSkippingRecord = errors.New("skipping record")
)

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
type Outbox[T any] interface {
	// SendTx stores the provided message within the provided Tx
	SendTx(ctx context.Context, tx *sql.Tx, msg T) error
}

type outbox[T any] struct {
	store Store
	mb    MessageBroker[T]
	dlq   DeadLetterQueue
	ed    EncodeDecoder[T]

	logger *slog.Logger

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

func WithLogger[T any](logger log.Logger) option[T] {
	return func(o *outbox[T]) {
		o.logger = slog.Default()
	}
}

func WithSlogLogger[T any](logger *slog.Logger) option[T] {
	return func(o *outbox[T]) {
		if logger == nil {
			logger = slog.Default()
		}

		o.logger = logger
	}
}

func New[T any](s Store, mb MessageBroker[T], dlq DeadLetterQueue, opts ...option[T]) Outbox[T] {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))

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

	ob.logger = ob.logger.With(
		slog.String("component", "outbox"),
		slog.String("messageBroker", reflect.TypeOf(mb).String()),
	)

	go ob.dispatch()

	return ob
}

func (o outbox[T]) SendTx(ctx context.Context, tx *sql.Tx, msg T) error {
	raw, err := o.ed.Encode(msg)
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
		o.logger.Error(
			"Failed to process message",
			slog.Group(
				"error",
				slog.String("message", err.Error()),
			),
		)
	}
}

func (o outbox[T]) processMessageTx(ctx context.Context, id xid.ID) func(s Store) bool {
	return func(s Store) (success bool) {
		logger := o.logger.With(slog.String("recordId", id.String()))

		fn := func() (err error) {
			var (
				msg    T
				record *Record
			)

			defer func() {
				if record == nil || err == nil || errors.Is(err, errSkippingRecord) {
					return
				}

				if e := s.Update(ctx, record); e != nil {
					err = errors.Join(err, fmt.Errorf("update record: %v", e))
					return
				}

				if record.NumberOfAttempts < o.maxRetries {
					return
				}

				logger.InfoContext(ctx, "Max retries hit, sending to DLQ")
				if e := o.dlq.Send(ctx, msg, err); e != nil {
					err = errors.Join(err, fmt.Errorf("send record to DLQ: %v", e))
					return
				}

				if e := s.Delete(ctx, record.ID); e != nil {
					err = errors.Join(fmt.Errorf("delete record after DLQ send: %v", e))
				}
			}()

			record, err = s.GetWithLock(ctx, id)
			if errors.Is(err, ErrRecordNotFound) {
				return fmt.Errorf("record not found, %w", errSkippingRecord)
			} else if err != nil {
				return fmt.Errorf("get with lock: %v", err)
			}

			if record.LastAttemptAt != nil && time.Since(*record.LastAttemptAt) < 90*time.Second {
				return fmt.Errorf("too soon since last attempt, %w", errSkippingRecord)
			}

			msg, err = o.ed.Decode(record.Message)
			if err != nil {
				return fmt.Errorf("decode message: %v", err)
			}

			mbCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
			defer cancel()

			if err = o.mb.Send(mbCtx, msg); err != nil {
				attemptedAt := time.Now()
				record.LastAttemptAt = &attemptedAt
				record.NumberOfAttempts++
				return fmt.Errorf("message bus send: %v", err)
			}

			if err := s.Delete(ctx, record.ID); err != nil {
				return fmt.Errorf("delete record %q: %v", record.ID, err)
			}

			return nil
		}

		err := fn()
		if err == nil {
			logger.InfoContext(
				ctx,
				"Successfully processed message",
			)
			return true
		}

		if errors.Is(err, errSkippingRecord) {
			logger.InfoContext(
				ctx,
				"Skipping message",
				slog.String("reason", err.Error()),
			)
			return true
		}

		logger.ErrorContext(
			ctx,
			"Failed to process outbox record",
			slog.Group(
				"error",
				slog.String("message", err.Error()),
			),
		)

		return false
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
