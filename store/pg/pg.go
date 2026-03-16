package pg

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/nrfta/go-outbox"

	"github.com/lib/pq"
	"github.com/rs/xid"
)

type execQuerier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

type Store struct {
	db             execQuerier
	tableName      string
	connStr        string
	chanName       string
	logger         *slog.Logger
	pageSize       int
	chanBufferSize int
}

type option func(s *Store)

func WithTableName(tn string) option {
	return func(s *Store) {
		s.tableName = tn
	}
}

func WithLogger(logger *slog.Logger) option {
	return func(s *Store) {
		if logger == nil {
			logger = slog.Default()
		}

		s.logger = logger
	}
}

func WithPageSize(n int) option {
	return func(s *Store) {
		if n > 0 {
			s.pageSize = n
		}
	}
}

func WithChannelBufferSize(n int) option {
	return func(s *Store) {
		if n > 0 {
			s.chanBufferSize = n
		}
	}
}

var _ outbox.Store = &Store{}

func NewStore(db execQuerier, connStr string, opts ...option) (*Store, error) {
	s := &Store{
		db:             db,
		tableName:      "outbox",
		connStr:        connStr,
		chanName:       "",
		logger:         slog.Default(),
		pageSize:       6,
		chanBufferSize: 5,
	}

	for _, o := range opts {
		o(s)
	}

	s.logger = s.logger.With(
		slog.String("component", "outbox/pgStore"),
		slog.String("tableName", s.tableName),
	)

	s.chanName = strings.ReplaceAll(
		fmt.Sprintf("%s_channel", s.tableName),
		".",
		"_",
	)

	if err := s.init(); err != nil {
		return nil, err
	}

	return s, nil
}

func (s Store) CreateRecordTx(ctx context.Context, tx *sql.Tx, r outbox.Record) (*outbox.Record, error) {
	query := fmt.Sprintf(`
	INSERT INTO %s VALUES ($1, $2);
	`, s.tableName)

	r.ID = xid.New()
	if _, err := tx.ExecContext(ctx, query, r.ID.String(), r.Message); err != nil {
		return nil, err
	}

	return &r, nil
}

func (s Store) Listen() <-chan xid.ID {
	var (
		logger   = s.logger.With("method", "Listen")
		listener = pq.NewListener(
			s.connStr,
			15*time.Second,
			2*time.Minute,
			func(event pq.ListenerEventType, err error) {},
		)
	)

	if err := listener.Listen(s.chanName); err != nil {
		logger.Error(
			"unable to listen to channel",
			"chanName",
			s.chanName,
			"error",
			err,
		)
		return nil
	}

	logger.Info(
		"listening on channel",
		"chanName",
		s.chanName,
	)

	// Ping the listner every 90 seconds to ensure it stays connected and receives notifications in a timely manner.
	pingTicker := time.NewTicker(90 * time.Second)
	go func(l *pq.Listener) {
		for range pingTicker.C {
			if err := l.Ping(); err != nil {
				logger.Error("error pinging listener", "error", err)
			}
		}
	}(listener)

	idChan := make(chan xid.ID, s.chanBufferSize)
	go func(l *pq.Listener) {
		for {
			err := s.getRecordIDs(idChan)
			if err != nil {
				s.logger.Error("unable to get record ids", "error", err)
				continue
			}

			select {
			case <-l.Notify:
				// New record(s) available to process
			case <-time.After(90 * time.Second):
				// Check if there's more work available, just in case it takes a while
				// for the Listener to notice connection loss and reconnect.
			}
		}
	}(listener)

	return idChan
}

func (s Store) getRecordIDs(idChan chan xid.ID) error {
	var lastID string
	for {
		n, err := s.fetchPage(idChan, &lastID)
		if err != nil {
			return err
		}
		if n == 0 {
			return nil
		}
	}
}

func (s Store) fetchPage(idChan chan xid.ID, lastID *string) (int, error) {
	ids, err := s.queryPage(*lastID)
	if err != nil {
		return 0, err
	}

	// Send to the channel outside of the DB context so that blocking on a
	// full channel does not hold open rows or trigger a context timeout.
	for _, id := range ids {
		idChan <- id
	}

	if len(ids) > 0 {
		*lastID = ids[len(ids)-1].String()
	}

	return len(ids), nil
}

// queryPage executes a single keyset-paginated query and returns up to
// pageSize IDs. The context timeout only covers the DB round-trip; channel
// backpressure cannot cause it to expire.
func (s Store) queryPage(afterID string) ([]xid.ID, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var query string
	var args []any
	if afterID == "" {
		query = fmt.Sprintf(`SELECT id FROM %s ORDER BY id LIMIT %d;`, s.tableName, s.pageSize)
	} else {
		query = fmt.Sprintf(`SELECT id FROM %s WHERE id > $1 ORDER BY id LIMIT %d;`, s.tableName, s.pageSize)
		args = []any{afterID}
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []xid.ID
	for rows.Next() {
		var rawID string
		if err := rows.Scan(&rawID); err != nil {
			return nil, err
		}

		id, err := xid.FromString(rawID)
		if err != nil {
			return nil, err
		}

		ids = append(ids, id)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return ids, nil
}

func (s Store) GetWithLock(ctx context.Context, id xid.ID) (*outbox.Record, error) {
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

func (s Store) Delete(ctx context.Context, id xid.ID) error {
	query := fmt.Sprintf(`
	DELETE FROM %s WHERE id = $1;
	`, s.tableName)

	_, err := s.db.ExecContext(ctx, query, id.String())
	if err != nil {
		return err
	}

	return nil
}

func (s Store) ProcessTx(ctx context.Context, fn func(outbox.Store) bool) error {
	db, ok := s.db.(interface {
		BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
	})
	if !ok {
		return errors.New("process transaction can only be called at the parent level")
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("unable to create transaction: %v", err)
	}
	defer tx.Rollback() // no-op after Commit; silently handles context cancellation

	store := Store{
		db:             tx,
		tableName:      s.tableName,
		logger:         s.logger,
		pageSize:       s.pageSize,
		chanBufferSize: s.chanBufferSize,
	}

	if success := fn(store); !success {
		return nil // rollback handled by defer; real error already logged in callback
	}

	return tx.Commit()
}

func (s Store) Update(ctx context.Context, record *outbox.Record) error {
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

func (s Store) init() error {
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
