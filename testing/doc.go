// Package testing provides synchronous test implementations of outbox pattern interfaces.
//
// This package enables deterministic, synchronous event processing in tests by replacing
// asynchronous components (background workers, NATS JetStream) with synchronous equivalents.
// Messages are processed immediately when created, eliminating race conditions and making
// tests more reliable and easier to debug.
//
// # Key Components
//
// SyncStore: Implements outbox.Store but immediately sends messages instead of storing
// them for async processing. Messages are decoded and sent synchronously to the broker.
//
// SyncBroker: Implements outbox.MessageBroker[*nats.Msg] and routes messages to
// SyncJetStream for immediate delivery to consumers.
//
// SyncJetStream: Implements jetstream.JetStream but delivers messages synchronously
// to registered consumers, bypassing NATS infrastructure entirely.
//
// # Usage Example
//
//	// Setup synchronous testing infrastructure
//	syncJS := testing.NewSyncJetStream()
//	syncBroker := testing.NewSyncBroker(syncJS)
//	syncStore := testing.NewSyncStore(syncBroker)
//
//	// Create outbox with sync components
//	outbox, err := outbox.New(syncStore, syncBroker)
//	if err != nil {
//	    panic(err)
//	}
//
//	// Override DI container with sync JetStream
//	do.Override(container, func(i *do.Injector) (jetstream.JetStream, error) {
//	    return syncJS, nil
//	})
//
//	// Events will now be processed synchronously and deterministically
//	// No need to wait for async workers or poll for message delivery
//
// # Benefits
//
// - Deterministic execution: No async race conditions
// - Immediate processing: Events handled synchronously when created
// - Full stack traces: See the complete call chain in test failures
// - No infrastructure: Tests run without NATS or background workers
// - Simpler debugging: Step through event handling in debugger
//
// # Important Limitations
//
// Transaction Semantics: SyncStore sends messages immediately when CreateRecordTx is
// called, BEFORE the transaction commits. This differs from the real outbox pattern:
//
//	Real Outbox: Store → Commit → NOTIFY → Worker → Send
//	SyncStore:   Send → Store (no-op) → Commit (too late)
//
// This means messages are sent even if the transaction is later rolled back, violating
// transactional guarantees. This is acceptable for most test scenarios where:
//   - Tests use transaction-per-test isolation (go-txdb)
//   - Focus is on business logic, not transaction edge cases
//   - Rollback scenarios with events are not being tested
//
// Async Behavior: This approach tests business logic but not async behavior of the
// real system. For integration tests that verify async messaging, transaction rollback
// scenarios, or true transactional semantics, use real PostgreSQL outbox store with
// NATS JetStream via testcontainers.
package testing
