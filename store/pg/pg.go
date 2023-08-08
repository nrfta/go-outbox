package pg

//go:generate go run go.uber.org/mock/mockgen --destination=mock_pg_test.go -package=pg -self_package=github.com/nrfta/go-outbox/store/pg . Logger

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/nrfta/go-outbox"

	"github.com/lib/pq"
	"github.com/rs/xid"
)

type execQuerier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

type Logger log.Logger

type pgStore struct {
	db        execQuerier
	tableName string
	connStr   string
	chanName  string
	logger    Logger
}

type option func(s *pgStore)

func WithTableName(tn string) option {
	return func(s *pgStore) {
		s.tableName = tn
	}
}

func WithLogger(l Logger) option {
	return func(s *pgStore) {
		s.logger = l
	}
}

var _ outbox.Store = &pgStore{}

func NewStore(db execQuerier, connStr string, logger Logger, opts ...option) (*pgStore, error) {
	s := &pgStore{db, "outbox", connStr, "", logger}

	for _, o := range opts {
		o(s)
	}

	s.chanName = strings.ReplaceAll(fmt.Sprintf("%s_channel", s.tableName), ".", "_")

	if err := s.init(); err != nil {
		return nil, err
	}

	return s, nil
}

func (s pgStore) CreateRecordTx(ctx context.Context, tx *sql.Tx, r outbox.Record) (*outbox.Record, error) {
	query := fmt.Sprintf(`
	INSERT INTO %s VALUES ($1, $2);
	`, s.tableName)

	r.ID = xid.New()
	if _, err := tx.ExecContext(ctx, query, r.ID.String(), r.Message); err != nil {
		return nil, err
	}

	return &r, nil
}

func (s pgStore) Listen() <-chan xid.ID {
	var (
		logger   = log.With(s.logger, "component", "pgStore", "method", "Listen")
		listener = pq.NewListener(
			s.connStr,
			15*time.Second,
			2*time.Minute,
			func(event pq.ListenerEventType, err error) {},
		)
	)

	if err := listener.Listen(s.chanName); err != nil {
		logger.Log("err", fmt.Errorf("unable to listen to channel %s: %v", s.chanName, err))
		return nil
	}

	logger.Log("msg", fmt.Sprintf("listening on channel %s", s.chanName))

	idChan := make(chan xid.ID, 1)
	go func(l *pq.Listener) {
		for {
			ids, err := s.getRecordIDs()
			if err != nil {
				s.logger.Log("err", err)
				continue
			}

			for _, i := range ids {
				idChan <- i
			}

			select {
			case <-l.Notify:
				// New record(s) available to process
			case <-time.After(90 * time.Second):
				go l.Ping()
				// Check if there's more work available, just in case it takes a while
				// for the Listener to notice connection loss and reconnect.
			}
		}
	}(listener)

	return idChan
}

func (s pgStore) getRecordIDs() ([]xid.ID, error) {
	var res []xid.ID
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	query := fmt.Sprintf(`
	SELECT id FROM %s;
	`, s.tableName)

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var rawID string
		if err := rows.Scan(&rawID); err != nil {
			return nil, err
		}

		id, err := xid.FromString(rawID)
		if err != nil {
			return nil, err
		}

		res = append(res, id)
	}

	return res, nil
}

func (s pgStore) GetWithLock(ctx context.Context, id xid.ID) (*outbox.Record, error) {
	if _, ok := s.db.(*sql.Tx); !ok {
		return nil, errors.New("get method must be called inside a transaction")
	}

	query := fmt.Sprintf(`
	SELECT id, data, create_at, num_of_attempts, last_attempted_at
	FROM %s
	WHERE id = $1
	FOR UPDATE SKIP LOCKED;
	`, s.tableName)

	var res outbox.Record
	if err := s.db.QueryRowContext(ctx, query, id.String()).
		Scan(
			&res.ID,
			&res.Message,
			&res.CreatedAt,
			&res.NumberOfAttempts,
			&res.LastAttemptAt,
		); err != nil && errors.Is(err, sql.ErrNoRows) {
		return nil, outbox.ErrRecordNotFound
	} else if err != nil {
		return nil, err
	}

	return &res, nil
}

func (s pgStore) Delete(ctx context.Context, id xid.ID) error {
	query := fmt.Sprintf(`
	DELETE FROM %s WHERE id = $1;
	`, s.tableName)

	_, err := s.db.ExecContext(ctx, query, id.String())
	if err != nil {
		return err
	}

	return nil
}

func (s pgStore) ProcessTx(ctx context.Context, fn func(outbox.Store) bool) error {
	db, ok := s.db.(*sql.DB)
	if !ok {
		return errors.New("process transaction can only be called at the parent level")
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("unable to create transaction: %v", err)
	}

	store := pgStore{
		db:        tx,
		tableName: s.tableName,
	}

	if success := fn(store); !success {
		return tx.Rollback()
	}

	return tx.Commit()
}

func (s pgStore) Update(ctx context.Context, record *outbox.Record) error {
	if record == nil {
		return errors.New("record cannot be nil")
	}

	query := fmt.Sprintf(`
	UPDATE %s
	SET num_of_attempts = $1, last_attempted_at = $2
	WHERE id = $3
	`, s.tableName)

	if _, err := s.db.ExecContext(
		ctx,
		query,
		record.NumberOfAttempts,
		record.LastAttemptAt,
		record.ID,
	); err != nil {
		return err
	}

	return nil
}

func (s pgStore) init() error {
	var (
		fnName      = strings.ReplaceAll(fmt.Sprintf("notify_%s_channel", s.tableName), ".", "_")
		triggerName = strings.ReplaceAll(fmt.Sprintf("%s_insert_notification", s.tableName), ".", "_")
		query       = fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id TEXT PRIMARY KEY,
			data bytea,
			create_at timestamp DEFAULT NOW(),
			num_of_attempts int DEFAULT 0,
			last_attempted_at timestamp
		);
			
		CREATE OR REPLACE FUNCTION %s() 
			RETURNS TRIGGER 
			LANGUAGE PLPGSQL
		AS $$
		BEGIN
			NOTIFY %s;
			RETURN NULL;
		END;
		$$
		;
			
		DROP TRIGGER IF EXISTS %s ON %s;
			
		CREATE TRIGGER %s AFTER INSERT
			ON %s
			FOR EACH ROW
			EXECUTE PROCEDURE %s();
		`,
			s.tableName,
			fnName,
			s.chanName,
			triggerName,
			s.tableName,
			triggerName,
			s.tableName,
			fnName,
		)
	)

	_, err := s.db.ExecContext(context.Background(), query)
	if err != nil {
		return err
	}

	return nil
}
