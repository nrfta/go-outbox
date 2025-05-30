# go-outbox

go-outbox implements the [outbox pattern](https://microservices.io/patterns/data/transactional-outbox.html) for Go applications.  It stores messages in a database table as part of the same transaction as your business logic and dispatches them asynchronously to your message broker.

## Features

- Generic API that works with any message type
- Built in PostgreSQL store with notification triggers
- Message broker integrations for **Kafka** (franz-go) and **NATS** (including JetStream)
- Pluggable dead letter queue interface
- Configurable concurrency and retry behaviour
- Uses `encoding/gob` for message serialization by default

## Installation

```sh
go get github.com/nrfta/go-outbox
```

## Quick start

The snippet below shows how to configure an outbox using PostgreSQL and NATS JetStream:

```go
package main

import (
    "database/sql"
    "log/slog"

    "github.com/nats-io/nats.go"
    "github.com/nats-io/nats.go/jetstream"

    "github.com/nrfta/go-outbox"
    mbNats "github.com/nrfta/go-outbox/mb/nats"
    outboxPg "github.com/nrfta/go-outbox/store/pg"
)

func NewNatsOutbox(db *sql.DB, connStr string) (outbox.Outbox[*nats.Msg], error) {
    nc, err := nats.Connect(nats.DefaultURL)
    if err != nil {
        return nil, err
    }

    js, err := jetstream.New(nc)
    if err != nil {
        return nil, err
    }

    broker, err := mbNats.NewJetstream(js)
    if err != nil {
        return nil, err
    }

    store, err := outboxPg.NewStore(db, connStr)
    if err != nil {
        return nil, err
    }

    ob := outbox.New[*nats.Msg](
        store,
        broker,
        logDeadLetterQueue{},
        outbox.WithLogger[*nats.Msg](slog.Default()),
        outbox.WithMaxRetries[*nats.Msg](10),
        outbox.WithNumberOfRoutines[*nats.Msg](5),
    )
    return ob, nil
}
```

Sending a message is then as simple as storing it within your transaction:

```go
ctx := context.Background()
tx, _ := db.Begin()

msg := &nats.Msg{Subject: "hello", Data: []byte("world")}
if err := ob.SendTx(ctx, tx, msg); err != nil {
    tx.Rollback()
    return err
}
return tx.Commit()
```

## Options

The `outbox.New` constructor accepts several optional configuration functions:

- `WithNumberOfRoutines(n int)` – limits the number of goroutines used to dispatch messages
- `WithMaxRetries(n int)` – maximum number of attempts before sending a message to the dead letter queue
- `WithLogger(*slog.Logger)` – provide a custom logger

You can provide your own store or message broker by implementing the `Store` and `MessageBroker` interfaces found in [outbox.go](outbox.go).

## Running tests

Integration tests require a running PostgreSQL instance.  You can run all tests with:

```sh
go test ./...
```

The CI workflow spins up Postgres automatically.

