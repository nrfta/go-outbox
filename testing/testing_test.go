package testing_test

import (
	"bytes"
	"context"
	"encoding/gob"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/nrfta/go-outbox"
	outboxTesting "github.com/nrfta/go-outbox/testing"
)

func TestTestingSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Testing Suite")
}

var _ = Describe("Sync Testing Utilities", func() {
	var (
		syncJS     *outboxTesting.SyncJetStream
		syncBroker *outboxTesting.SyncBroker
		syncStore  *outboxTesting.SyncStore
		ctx        context.Context
	)

	BeforeEach(func() {
		ctx = context.Background()
		syncJS = outboxTesting.NewSyncJetStream()
		syncBroker = outboxTesting.NewSyncBroker(syncJS)
		syncStore = outboxTesting.NewSyncStore(syncBroker)
	})

	Describe("SyncStore", func() {
		It("implements outbox.Store interface", func() {
			var _ outbox.Store = syncStore
		})

		It("immediately sends messages to broker when CreateRecordTx is called", func() {
			subject := "test.subject"
			data := []byte("test data")
			messageReceived := false

			// Setup consumer to capture message
			consumer, err := syncJS.CreateOrUpdateConsumer(ctx, "test-stream", jetstream.ConsumerConfig{
				FilterSubject: subject,
			})
			Expect(err).ToNot(HaveOccurred())

			// Register handler
			_, err = consumer.Consume(func(msg jetstream.Msg) {
				Expect(msg.Subject()).To(Equal(subject))
				Expect(msg.Data()).To(Equal(data))
				messageReceived = true
				msg.Ack()
			})
			Expect(err).ToNot(HaveOccurred())

			// Create NATS message and encode it (like outbox does)
			msg := &nats.Msg{
				Subject: subject,
				Data:    data,
			}

			var buf bytes.Buffer
			encoder := gob.NewEncoder(&buf)
			err = encoder.Encode(msg)
			Expect(err).ToNot(HaveOccurred())

			// Create record and send via store
			record := outbox.Record{
				Message: buf.Bytes(),
			}

			_, err = syncStore.CreateRecordTx(ctx, nil, record)
			Expect(err).ToNot(HaveOccurred())

			// Message should be immediately received
			Expect(messageReceived).To(BeTrue())
		})
	})

	Describe("SyncBroker", func() {
		It("implements outbox.MessageBroker interface", func() {
			var _ outbox.MessageBroker[*nats.Msg] = syncBroker
		})

		It("routes messages to SyncJetStream", func() {
			subject := "test.routing"
			data := []byte("routing test")
			messageReceived := false

			// Setup consumer
			consumer, err := syncJS.CreateOrUpdateConsumer(ctx, "test-stream", jetstream.ConsumerConfig{
				FilterSubject: subject,
			})
			Expect(err).ToNot(HaveOccurred())

			// Setup handler
			_, err = consumer.Consume(func(msg jetstream.Msg) {
				Expect(msg.Subject()).To(Equal(subject))
				Expect(msg.Data()).To(Equal(data))
				messageReceived = true
				msg.Ack()
			})
			Expect(err).ToNot(HaveOccurred())

			// Send message via broker
			msg := &nats.Msg{
				Subject: subject,
				Data:    data,
			}
			err = syncBroker.Send(ctx, msg)
			Expect(err).ToNot(HaveOccurred())
			Expect(messageReceived).To(BeTrue())
		})
	})

	Describe("SyncJetStream", func() {
		It("implements jetstream.JetStream interface", func() {
			var _ jetstream.JetStream = syncJS
		})

		It("delivers messages to consumers synchronously", func() {
			subject := "test.sync"
			data := []byte("sync test")
			messageReceived := false

			// Create consumer
			consumer, err := syncJS.CreateOrUpdateConsumer(ctx, "test-stream", jetstream.ConsumerConfig{
				FilterSubject: subject,
			})
			Expect(err).ToNot(HaveOccurred())

			// Register handler
			_, err = consumer.Consume(func(msg jetstream.Msg) {
				Expect(msg.Subject()).To(Equal(subject))
				Expect(msg.Data()).To(Equal(data))
				messageReceived = true
				msg.Ack()
			})
			Expect(err).ToNot(HaveOccurred())

			// Send message - should be delivered immediately and synchronously
			err = syncJS.SendMessage(ctx, subject, data)
			Expect(err).ToNot(HaveOccurred())

			// Message should already be received (no waiting needed)
			Expect(messageReceived).To(BeTrue())
		})

		It("matches wildcard subjects correctly", func() {
			wildcard := "events.*"
			specificSubject := "events.created"
			messageReceived := false

			// Create consumer with wildcard
			consumer, err := syncJS.CreateOrUpdateConsumer(ctx, "test-stream", jetstream.ConsumerConfig{
				FilterSubject: wildcard,
			})
			Expect(err).ToNot(HaveOccurred())

			// Register handler
			_, err = consumer.Consume(func(msg jetstream.Msg) {
				Expect(msg.Subject()).To(Equal(specificSubject))
				messageReceived = true
				msg.Ack()
			})
			Expect(err).ToNot(HaveOccurred())

			// Send message with specific subject
			err = syncJS.SendMessage(ctx, specificSubject, []byte("test"))
			Expect(err).ToNot(HaveOccurred())

			Expect(messageReceived).To(BeTrue())
		})

		It("handles no matching consumer gracefully", func() {
			// Send message with no consumer registered
			err := syncJS.SendMessage(ctx, "unregistered.subject", []byte("test"))
			Expect(err).ToNot(HaveOccurred())
		})
	})

	Describe("End-to-end flow", func() {
		It("processes messages synchronously from store to consumer", func() {
			subject := "e2e.test"
			data := []byte("end to end test")
			messageReceived := false

			// Setup consumer
			consumer, err := syncJS.CreateOrUpdateConsumer(ctx, "e2e-stream", jetstream.ConsumerConfig{
				FilterSubject: subject,
			})
			Expect(err).ToNot(HaveOccurred())

			// Register handler
			_, err = consumer.Consume(func(msg jetstream.Msg) {
				Expect(msg.Subject()).To(Equal(subject))
				Expect(msg.Data()).To(Equal(data))
				messageReceived = true
				msg.Ack()
			})
			Expect(err).ToNot(HaveOccurred())

			// Create and encode message
			msg := &nats.Msg{
				Subject: subject,
				Data:    data,
			}

			var buf bytes.Buffer
			encoder := gob.NewEncoder(&buf)
			err = encoder.Encode(msg)
			Expect(err).ToNot(HaveOccurred())

			// Send via store (simulates full outbox flow)
			record := outbox.Record{
				Message: buf.Bytes(),
			}
			_, err = syncStore.CreateRecordTx(ctx, nil, record)
			Expect(err).ToNot(HaveOccurred())

			// Message should be immediately processed
			Expect(messageReceived).To(BeTrue())
		})
	})
})
