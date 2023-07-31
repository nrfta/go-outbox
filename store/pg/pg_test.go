package pg

import (
	"context"
	"database/sql"
	"errors"
	"go-outbox"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/rs/xid"
)

var _ = Describe("pgStore", func() {
	Describe("NewStore", func() {
		It("should successfully create pgStore", func() {
			subject, err := NewStore(db, connStr, nil)
			Expect(err).To(Succeed())
			Expect(subject).ToNot(BeNil())
		})
	})

	Describe("#CreateRecordTx", func() {
		var (
			subject *pgStore
			ctx     context.Context
			tx      *sql.Tx

			record = outbox.Record{Message: []byte("data")}
		)

		BeforeEach(func() {
			subject = createStore()
			ctx = context.Background()
			tx, _ = db.BeginTx(ctx, nil)
		})

		It("should save the provided record on a successfull transaction", func() {
			res, err := subject.CreateRecordTx(ctx, tx, record)
			Expect(err).To(Succeed())
			tx.Commit()

			_, err = getRecord(db, res.ID)
			Expect(err).To(Succeed())
		})

		It("should not save the provided record on a failed transaction", func() {
			res, err := subject.CreateRecordTx(ctx, tx, record)
			Expect(err).To(Succeed())
			tx.Rollback()

			_, err = getRecord(db, res.ID)
			Expect(err).To(MatchError(sql.ErrNoRows))
		})
	})

	Describe("#GetWithLock", func() {
		var (
			subject *pgStore
			ctx     context.Context
			id      xid.ID
		)

		BeforeEach(func() {
			subject = createStore()
			ctx = context.Background()
			id = xid.New()

			err := insertRecord(db, outbox.Record{ID: id, Message: []byte("data")})
			Expect(err).To(Succeed())
		})

		It("should return a record on a valid id", func() {
			subject.ProcessTx(ctx, func(s outbox.Store) bool {
				res, err := s.GetWithLock(ctx, id)
				Expect(err).To(Succeed())
				Expect(res.CreatedAt).ToNot(BeZero())
				Expect(res.NumberOfAttempts).To(Equal(0))
				Expect(res.LastAttemptAt).To(BeNil())

				return true
			})
		})

		It("should return a `not found` error on invalid id", func() {
			subject.ProcessTx(ctx, func(s outbox.Store) bool {
				_, err := s.GetWithLock(ctx, xid.New())
				Expect(err).To(MatchError(outbox.ErrRecordNotFound))

				return true
			})
		})
	})

	Describe("#Listen", func() {
		It("should pull all new records on start", func() {
			var (
				subject = createStore()
				ids     = []xid.ID{xid.New(), xid.New(), xid.New()}
			)

			for _, id := range ids {
				_ = insertRecord(db, outbox.Record{ID: id, Message: []byte("data")})
			}

			output := make(chan xid.ID, len(ids)+1)
			idChan := subject.Listen()
			go func() {
				for res := range idChan {
					output <- res
				}
			}()

			Eventually(output).Should(HaveLen(len(ids)), "output")

			outputSlice := []xid.ID{}
			for len(output) > 0 {
				outputSlice = append(outputSlice, <-output)
			}

			Expect(outputSlice).To(ContainElements(ids))
		})

		It("should send a new id to the returned channel when a new record is created", func() {
			var (
				subject = createStore()
				idChan  = subject.Listen()
				ctx     = context.Background()
				tx, _   = db.BeginTx(ctx, nil)
			)

			res, err := subject.CreateRecordTx(ctx, tx, outbox.Record{})
			tx.Commit()

			Expect(err).To(Succeed())
			Eventually(idChan).Should(Receive(Equal(res.ID)))
		})
	})

	Describe("#Delete", func() {
		It("should successfully delete a valid record", func() {
			var (
				subject = createStore()
				id      = xid.New()
			)

			_ = insertRecord(db, outbox.Record{ID: id, Message: []byte("data")})

			err := subject.Delete(context.Background(), id)
			Expect(err).To(Succeed())

			_, err = getRecord(db, id)
			Expect(err).To(MatchError(sql.ErrNoRows))
		})
	})

	Describe("#ProcessTx", func() {
		It("should allow for locking of records for update", func() {
			var (
				subject = createStore()
				id      = xid.New()
			)

			_ = insertRecord(db, outbox.Record{ID: id, Message: []byte("data")})

			var notFoundCount int32 = 0
			deleteRecord := func() {
				defer GinkgoRecover()

				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				txErr := subject.ProcessTx(ctx, func(s outbox.Store) bool {
					record, err := s.GetWithLock(ctx, id)
					if errors.Is(err, outbox.ErrRecordNotFound) {
						atomic.AddInt32(&notFoundCount, 1)
						return true
					}
					Expect(err).To(Succeed())

					err = s.Delete(ctx, record.ID)
					Expect(err).To(Succeed())

					return true
				})
				Expect(txErr).To(Succeed())
			}

			wg := sync.WaitGroup{}
			var routineCount int32 = 10
			for i := 0; i < int(routineCount); i++ {
				wg.Add(1)
				go func() {
					deleteRecord()
					wg.Done()
				}()
			}

			wg.Wait()
			Expect(notFoundCount).To(Equal(routineCount - 1))
		})
	})

	Describe("#Update", func() {
		It("should update the record successfully", func() {
			var (
				subject         = createStore()
				id              = xid.New()
				lastAttemptedAt = time.Now()
				record          = outbox.Record{
					ID:               id,
					Message:          []byte("data"),
					NumberOfAttempts: 1,
					LastAttemptAt:    &lastAttemptedAt,
				}
			)

			_ = insertRecord(db, record)

			err := subject.Update(context.Background(), &record)
			Expect(err).To(Succeed())

			res, _ := getRecord(db, id)
			Expect(res.NumberOfAttempts).To(Equal(1))
			Expect(res.LastAttemptAt).ToNot(BeNil())
			Expect(*res.LastAttemptAt).ToNot(BeTemporally("==", lastAttemptedAt))
		})
	})
})

func createStore() *pgStore {
	s, err := NewStore(db, connStr, nil)
	Expect(err).To(Succeed())
	return s
}

func getRecord(db *sql.DB, id xid.ID) (*outbox.Record, error) {
	var (
		res   outbox.Record
		query = `
		SELECT id, data, create_at, num_of_attempts, last_attempted_at
		FROM outbox 
		WHERE id = $1;`
	)

	if err := db.QueryRow(query, id.String()).
		Scan(
			&res.ID,
			&res.Message,
			&res.CreatedAt,
			&res.NumberOfAttempts,
			&res.LastAttemptAt,
		); err != nil {
		return nil, err
	}

	return &res, nil

}

func insertRecord(db *sql.DB, r outbox.Record) error {
	_, err := db.Exec("INSERT INTO outbox (id, data) VALUES ($1, $2)", r.ID, r.Message)
	if err != nil {
		return err
	}

	return nil
}
