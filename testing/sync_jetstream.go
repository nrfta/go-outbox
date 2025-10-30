package testing

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// SyncJetStream implements jetstream.JetStream for synchronous testing.
// Unlike real NATS JetStream which processes messages asynchronously, this implementation
// delivers messages synchronously directly to consumer handlers. This enables:
// - Deterministic test execution (no race conditions from async message processing)
// - Immediate event processing (no need to wait for async delivery)
// - Simpler test debugging (stack traces show the full call chain)
type SyncJetStream struct {
	consumers map[string]*SyncConsumer
	mu        sync.RWMutex
}

// NewSyncJetStream creates a new synchronous JetStream for testing
func NewSyncJetStream() *SyncJetStream {
	return &SyncJetStream{
		consumers: make(map[string]*SyncConsumer),
	}
}

// CreateOrUpdateConsumer implements jetstream.JetStream
func (s *SyncJetStream) CreateOrUpdateConsumer(ctx context.Context, stream string, cfg jetstream.ConsumerConfig) (jetstream.Consumer, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	consumer := &SyncConsumer{
		stream:  stream,
		subject: cfg.FilterSubject,
	}

	// Store consumer using stream and subject as key
	key := fmt.Sprintf("%s:%s", stream, cfg.FilterSubject)
	s.consumers[key] = consumer
	return consumer, nil
}

// SendMessage delivers a message to the appropriate consumer synchronously
func (s *SyncJetStream) SendMessage(ctx context.Context, subject string, data []byte) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Find matching consumer by subject
	for _, consumer := range s.consumers {
		if consumer.matchesSubject(subject) {
			msg := &SyncJetstreamMsg{
				subject: subject,
				data:    data,
			}

			// Call the handler synchronously - let panics bubble up to fail tests
			if consumer.handler != nil {
				consumer.handler(msg)
			}
			return nil
		}
	}

	// No consumer found - this is acceptable as not all events have consumers
	return nil
}

// Unimplemented methods required by jetstream.JetStream interface
func (s *SyncJetStream) CreateStream(ctx context.Context, cfg jetstream.StreamConfig) (jetstream.Stream, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *SyncJetStream) UpdateStream(ctx context.Context, cfg jetstream.StreamConfig) (jetstream.Stream, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *SyncJetStream) Stream(ctx context.Context, stream string) (jetstream.Stream, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *SyncJetStream) DeleteStream(ctx context.Context, stream string) error {
	return fmt.Errorf("not implemented")
}

func (s *SyncJetStream) StreamNameBySubject(ctx context.Context, subject string) (string, error) {
	return "", fmt.Errorf("not implemented")
}

func (s *SyncJetStream) Consumer(ctx context.Context, stream, consumer string) (jetstream.Consumer, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *SyncJetStream) DeleteConsumer(ctx context.Context, stream, consumer string) error {
	return fmt.Errorf("not implemented")
}

func (s *SyncJetStream) AccountInfo(ctx context.Context) (*jetstream.AccountInfo, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *SyncJetStream) Publish(ctx context.Context, subject string, data []byte, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *SyncJetStream) PublishMsg(ctx context.Context, msg *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *SyncJetStream) PublishAsync(subject string, data []byte, opts ...jetstream.PublishOpt) (jetstream.PubAckFuture, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *SyncJetStream) PublishMsgAsync(msg *nats.Msg, opts ...jetstream.PublishOpt) (jetstream.PubAckFuture, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *SyncJetStream) PublishAsyncPending() int {
	return 0
}

func (s *SyncJetStream) PublishAsyncComplete() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

func (s *SyncJetStream) CreateOrUpdateKeyValue(ctx context.Context, cfg jetstream.KeyValueConfig) (jetstream.KeyValue, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *SyncJetStream) KeyValue(ctx context.Context, bucket string) (jetstream.KeyValue, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *SyncJetStream) DeleteKeyValue(ctx context.Context, bucket string) error {
	return fmt.Errorf("not implemented")
}

func (s *SyncJetStream) CreateObjectStore(ctx context.Context, cfg jetstream.ObjectStoreConfig) (jetstream.ObjectStore, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *SyncJetStream) ObjectStore(ctx context.Context, bucket string) (jetstream.ObjectStore, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *SyncJetStream) DeleteObjectStore(ctx context.Context, bucket string) error {
	return fmt.Errorf("not implemented")
}

func (s *SyncJetStream) CleanupPublisher() {
	// No-op for testing
}

func (s *SyncJetStream) Conn() *nats.Conn {
	return nil
}

func (s *SyncJetStream) Options() jetstream.JetStreamOptions {
	return jetstream.JetStreamOptions{}
}

func (s *SyncJetStream) CreateConsumer(ctx context.Context, stream string, cfg jetstream.ConsumerConfig) (jetstream.Consumer, error) {
	return s.CreateOrUpdateConsumer(ctx, stream, cfg)
}

func (s *SyncJetStream) UpdateConsumer(ctx context.Context, stream string, cfg jetstream.ConsumerConfig) (jetstream.Consumer, error) {
	return s.CreateOrUpdateConsumer(ctx, stream, cfg)
}

func (s *SyncJetStream) OrderedConsumer(ctx context.Context, stream string, cfg jetstream.OrderedConsumerConfig) (jetstream.Consumer, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *SyncJetStream) PauseConsumer(ctx context.Context, stream string, consumer string, pauseUntil time.Time) (*jetstream.ConsumerPauseResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *SyncJetStream) ResumeConsumer(ctx context.Context, stream string, consumer string) (*jetstream.ConsumerPauseResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *SyncJetStream) CreateOrUpdatePushConsumer(ctx context.Context, stream string, cfg jetstream.ConsumerConfig) (jetstream.PushConsumer, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *SyncJetStream) CreatePushConsumer(ctx context.Context, stream string, cfg jetstream.ConsumerConfig) (jetstream.PushConsumer, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *SyncJetStream) UpdatePushConsumer(ctx context.Context, stream string, cfg jetstream.ConsumerConfig) (jetstream.PushConsumer, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *SyncJetStream) PushConsumer(ctx context.Context, stream string, consumer string) (jetstream.PushConsumer, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *SyncJetStream) CreateKeyValue(ctx context.Context, cfg jetstream.KeyValueConfig) (jetstream.KeyValue, error) {
	return s.CreateOrUpdateKeyValue(ctx, cfg)
}

func (s *SyncJetStream) UpdateKeyValue(ctx context.Context, cfg jetstream.KeyValueConfig) (jetstream.KeyValue, error) {
	return s.CreateOrUpdateKeyValue(ctx, cfg)
}

func (s *SyncJetStream) KeyValueStoreNames(ctx context.Context) jetstream.KeyValueNamesLister {
	return nil
}

func (s *SyncJetStream) KeyValueStores(ctx context.Context) jetstream.KeyValueLister {
	return nil
}

func (s *SyncJetStream) CreateOrUpdateObjectStore(ctx context.Context, cfg jetstream.ObjectStoreConfig) (jetstream.ObjectStore, error) {
	return s.CreateObjectStore(ctx, cfg)
}

func (s *SyncJetStream) UpdateObjectStore(ctx context.Context, cfg jetstream.ObjectStoreConfig) (jetstream.ObjectStore, error) {
	return s.CreateObjectStore(ctx, cfg)
}

func (s *SyncJetStream) ObjectStoreNames(ctx context.Context) jetstream.ObjectStoreNamesLister {
	return nil
}

func (s *SyncJetStream) ObjectStores(ctx context.Context) jetstream.ObjectStoresLister {
	return nil
}

func (s *SyncJetStream) CreateOrUpdateStream(ctx context.Context, cfg jetstream.StreamConfig) (jetstream.Stream, error) {
	return s.CreateStream(ctx, cfg)
}

func (s *SyncJetStream) ListStreams(ctx context.Context, opts ...jetstream.StreamListOpt) jetstream.StreamInfoLister {
	return nil
}

func (s *SyncJetStream) StreamNames(ctx context.Context, opts ...jetstream.StreamListOpt) jetstream.StreamNameLister {
	return nil
}

// SyncConsumer implements jetstream.Consumer for testing
type SyncConsumer struct {
	stream  string
	subject string
	handler jetstream.MessageHandler
}

// Consume implements jetstream.Consumer
func (c *SyncConsumer) Consume(handler jetstream.MessageHandler, opts ...jetstream.PullConsumeOpt) (jetstream.ConsumeContext, error) {
	c.handler = handler
	return &SyncConsumeContext{closed: make(chan struct{})}, nil
}

// matchesSubject checks if a message subject matches this consumer's filter
func (c *SyncConsumer) matchesSubject(msgSubject string) bool {
	// The consumer's subject filter may contain wildcards like "*"
	// Convert to simple matching logic
	pattern := c.subject
	parts := strings.Split(pattern, ".")
	msgParts := strings.Split(msgSubject, ".")

	if len(parts) != len(msgParts) {
		return false
	}

	for i := range parts {
		if parts[i] != "*" && parts[i] != ">" && parts[i] != msgParts[i] {
			return false
		}
	}

	return true
}

// Unimplemented methods required by jetstream.Consumer interface
func (c *SyncConsumer) Info(ctx context.Context) (*jetstream.ConsumerInfo, error) {
	return nil, fmt.Errorf("not implemented")
}

func (c *SyncConsumer) CachedInfo() *jetstream.ConsumerInfo {
	return nil
}

func (c *SyncConsumer) Fetch(batch int, opts ...jetstream.FetchOpt) (jetstream.MessageBatch, error) {
	return nil, fmt.Errorf("not implemented")
}

func (c *SyncConsumer) FetchBytes(maxBytes int, opts ...jetstream.FetchOpt) (jetstream.MessageBatch, error) {
	return nil, fmt.Errorf("not implemented")
}

func (c *SyncConsumer) FetchNoWait(batch int) (jetstream.MessageBatch, error) {
	return nil, fmt.Errorf("not implemented")
}

func (c *SyncConsumer) Messages(opts ...jetstream.PullMessagesOpt) (jetstream.MessagesContext, error) {
	return nil, fmt.Errorf("not implemented")
}

func (c *SyncConsumer) Next(opts ...jetstream.FetchOpt) (jetstream.Msg, error) {
	return nil, fmt.Errorf("not implemented")
}

// SyncConsumeContext implements jetstream.ConsumeContext
type SyncConsumeContext struct {
	closed chan struct{}
	once   sync.Once
}

func (c *SyncConsumeContext) Stop() {
	c.once.Do(func() {
		close(c.closed)
	})
}

func (c *SyncConsumeContext) Drain() {
	c.once.Do(func() {
		close(c.closed)
	})
}

func (c *SyncConsumeContext) Closed() <-chan struct{} {
	return c.closed
}

// SyncJetstreamMsg implements jetstream.Msg for testing
type SyncJetstreamMsg struct {
	subject string
	data    []byte
	acked   bool
}

func (m *SyncJetstreamMsg) Data() []byte {
	return m.data
}

func (m *SyncJetstreamMsg) Headers() nats.Header {
	return nil
}

func (m *SyncJetstreamMsg) Subject() string {
	return m.subject
}

func (m *SyncJetstreamMsg) Reply() string {
	return ""
}

func (m *SyncJetstreamMsg) Metadata() (*jetstream.MsgMetadata, error) {
	return nil, fmt.Errorf("not implemented")
}

func (m *SyncJetstreamMsg) Ack() error {
	m.acked = true
	return nil
}

func (m *SyncJetstreamMsg) DoubleAck(ctx context.Context) error {
	m.acked = true
	return nil
}

func (m *SyncJetstreamMsg) Nak() error {
	return nil
}

func (m *SyncJetstreamMsg) NakWithDelay(delay time.Duration) error {
	return nil
}

func (m *SyncJetstreamMsg) InProgress() error {
	return nil
}

func (m *SyncJetstreamMsg) Term() error {
	return nil
}

func (m *SyncJetstreamMsg) TermWithReason(reason string) error {
	return nil
}

// Ensure types implement their respective interfaces
var _ jetstream.JetStream = (*SyncJetStream)(nil)
var _ jetstream.Consumer = (*SyncConsumer)(nil)
var _ jetstream.ConsumeContext = (*SyncConsumeContext)(nil)
var _ jetstream.Msg = (*SyncJetstreamMsg)(nil)
