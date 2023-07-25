package outbox

//go:generate go run go.uber.org/mock/mockgen --source=outbox.go --destination=mock_outbox_test.go -package=outbox -self_package=go-outbox Store,Logger,MessageBroker

import (
	"context"
	"database/sql"
	"sync"
	"testing"

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
		mockCtrl          = gomock.NewController(GinkgoT())
		mockStore         = NewMockStore(mockCtrl)
		mockMessageBroker = NewMockMessageBroker(mockCtrl)
	)

	Describe("#SendTx", func() {
		It("should encode and save message", func() {
			var (
				ctx  = context.Background()
				tx   = &sql.Tx{}
				data = testMessage{Data: "testing"}
				raw  = []byte("output")
			)

			mockMessageBroker.EXPECT().EncodeMessage(data).Return(raw, nil)
			mockStore.EXPECT().
				CreateRecordTx(
					gomock.AssignableToTypeOf(ctx),
					gomock.AssignableToTypeOf(tx),
					Record{Data: raw},
				).Return(nil, nil)

			subject := outbox{
				store: mockStore,
				mb:    mockMessageBroker,
			}

			err := subject.SendTx(ctx, tx, data)
			Expect(err).To(Succeed())
		})
	})

	Describe("#dispatch", func() {
		It("should process all ids from store", func() {
			var (
				idChan     = make(chan xid.ID)
				ids        = []xid.ID{xid.New(), xid.New()}
				wg         = sync.WaitGroup{}
				mockLogger = NewMockLogger(mockCtrl)
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
			mockLogger.EXPECT().Log(gomock.Any(), gomock.Any()).Times(len(ids))

			subject := outbox{store: mockStore, numRoutines: 5, logger: mockLogger}

			go subject.dispatch()
			for _, i := range ids {
				idChan <- i
			}
			close(idChan)

			wg.Wait()
		})
	})

	Describe("#processMessageTx", func() {
		It("should get, send, then delete message", func() {
			var (
				data   = []byte("data")
				record = &Record{ID: xid.New(), Data: data}

				store = NewMockStore(mockCtrl)
			)

			store.EXPECT().GetForUpdate(gomock.Any(), record.ID).Return(record, nil)
			mockMessageBroker.EXPECT().Send(gomock.Any(), data).Return(nil)
			store.EXPECT().Delete(gomock.Any(), record.ID).Return(nil)

			subject := outbox{
				store: mockStore,
				mb:    mockMessageBroker,
			}

			success := subject.processMessageTx(context.Background(), record.ID)(store)
			Expect(success).To(BeTrue())
		})

		It("should skip the record if not found", func() {
			var (
				record = &Record{ID: xid.New(), Data: []byte("data")}

				store             = NewMockStore(mockCtrl)
				mockMessageBroker = NewMockMessageBroker(mockCtrl)
			)

			store.EXPECT().GetForUpdate(gomock.Any(), record.ID).Return(nil, ErrRecordNotFound)

			subject := outbox{
				store: mockStore,
				mb:    mockMessageBroker,
			}

			success := subject.processMessageTx(context.Background(), record.ID)(store)
			Expect(success).To(BeTrue())
		})
	})
})
