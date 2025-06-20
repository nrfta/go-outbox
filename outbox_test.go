package outbox

//go:generate go run go.uber.org/mock/mockgen -destination mock_outbox_test.go -package outbox -source outbox.go Store,MessageBroker,EncodeDecoder

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/rs/xid"
	"go.uber.org/mock/gomock"
)

type testMessage struct {
	Data string
}

func TestOutboxSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Outbox Suite")
}

var _ = Describe("Outbox", func() {
	var (
		mockCtrl          *gomock.Controller
		mockStore         *MockStore
		mockMessageBroker *MockMessageBroker[*testMessage]
		mockEncodeDecoder *MockEncodeDecoder[*testMessage]

		buf    = &bytes.Buffer{}
		logger *slog.Logger
	)

	BeforeEach(func() {
		mockCtrl = gomock.NewController(GinkgoT())
		mockStore = NewMockStore(mockCtrl)
		mockMessageBroker = NewMockMessageBroker[*testMessage](mockCtrl)
		mockEncodeDecoder = NewMockEncodeDecoder[*testMessage](mockCtrl)

		logger = slog.New(slog.NewJSONHandler(buf, nil))
	})

	Describe("#SendTx", func() {
		It("should encode and save message", func() {
			var (
				ctx  = context.Background()
				tx   = &sql.Tx{}
				data = &testMessage{Data: "testing"}
				raw  = []byte("output")
			)

			mockEncodeDecoder.EXPECT().Encode(data).Return(raw, nil)
			mockStore.EXPECT().
				CreateRecordTx(
					gomock.AssignableToTypeOf(ctx),
					gomock.AssignableToTypeOf(tx),
					Record{Message: raw},
				).Return(nil, nil)

			subject := outbox[*testMessage]{
				ed:    mockEncodeDecoder,
				store: mockStore,
			}

			err := subject.SendTx(ctx, tx, data)
			Expect(err).To(Succeed())
		})
	})

	Describe("#dispatch", func() {
		It("should process all ids from store", func() {
			var (
				idChan = make(chan xid.ID)
				ids    = []xid.ID{xid.New(), xid.New()}
				wg     = sync.WaitGroup{}
			)

			wg.Add(len(ids))
			mockStore.EXPECT().Listen().Return(idChan)
			mockStore.EXPECT().
				ProcessTx(gomock.Any(), gomock.Any()).
				Times(len(ids)).
				DoAndReturn(func(arg0, arg1 any) any {
					defer wg.Done()
					return nil
				})

			subject := outbox[testMessage]{store: mockStore, numRoutines: 5, logger: logger}

			go subject.dispatch()
			for _, i := range ids {
				idChan <- i
			}
			close(idChan)

			wg.Wait()
		})
	})

	Describe("#processMessageTx", func() {
		It("should get, decode, send, then delete message", func() {
			var (
				record = &Record{ID: xid.New(), Message: []byte{}}
				data   = &testMessage{Data: "data"}
			)

			mockStore.EXPECT().GetWithLock(gomock.Any(), record.ID).Return(record, nil)
			mockEncodeDecoder.EXPECT().Decode(record.Message).Return(data, nil)
			mockMessageBroker.EXPECT().Send(gomock.Any(), data).Return(nil)
			mockStore.EXPECT().Delete(gomock.Any(), record.ID).Return(nil)

			subject := outbox[*testMessage]{
				store:  mockStore,
				mb:     mockMessageBroker,
				ed:     mockEncodeDecoder,
				logger: logger,
			}

			success := subject.processMessageTx(context.Background(), record.ID)(mockStore)
			Expect(success).To(BeTrue())
		})

		It("should skip the record if not found", func() {
			var (
				record = &Record{ID: xid.New(), Message: []byte("data")}

				store             = NewMockStore(mockCtrl)
				mockMessageBroker = NewMockMessageBroker[testMessage](mockCtrl)
			)

			store.EXPECT().GetWithLock(gomock.Any(), record.ID).Return(nil, ErrRecordNotFound)

			subject := outbox[testMessage]{
				store:  mockStore,
				mb:     mockMessageBroker,
				logger: logger,
			}

			success := subject.processMessageTx(context.Background(), record.ID)(store)
			Expect(success).To(BeTrue())
		})

		It("should send to dead letter queue if max retries are hit", func() {
			var (
				record = &Record{ID: xid.New(), Message: []byte{}, CreatedAt: time.Now(), NumberOfAttempts: 4}
				data   = &testMessage{Data: "data"}
				err    = errors.New("send error")

				mockDLQ = NewMockDeadLetterQueue(mockCtrl)
			)

			mockStore.EXPECT().GetWithLock(gomock.Any(), record.ID).Return(record, nil)
			mockEncodeDecoder.EXPECT().Decode(record.Message).Return(data, nil)
			mockMessageBroker.EXPECT().Send(gomock.Any(), data).Return(err)
			mockStore.EXPECT().Update(gomock.Any(), record).Return(nil)
			mockDLQ.EXPECT().Send(gomock.Any(), data, fmt.Errorf("message bus send: %v", err))
			mockStore.EXPECT().Delete(gomock.Any(), record.ID).Return(nil)

			subject := outbox[*testMessage]{
				store:  mockStore,
				mb:     mockMessageBroker,
				ed:     mockEncodeDecoder,
				logger: logger,
				dlq:    mockDLQ,
			}

			success := subject.processMessageTx(context.Background(), record.ID)(mockStore)
			Expect(success).To(BeFalse())
		})
	})
})
