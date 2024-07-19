# Outbox

This package implements the outbox pattern for message queues.

`go-outbox`` is a Go library implementing the outbox pattern, designed to ensure reliable messaging between services using message brokers like Kafka, Nats, or any other supported message broker. This pattern helps achieve consistency between your database and the message broker by storing messages in a database outbox table before sending them to the broker in a transaction.

## Installation

To install the library, use the following command:

```sh
go get -u github.com/nrfta/go-outbox
```

## Setup

Below is an example of how to set up and use go-outbox with NATS as the message broker and PostgreSQL as the outbox store.

```go
package main

import (
    "database/sql"
    "log"
    "log/slog"
    "os"

    "github.com/nats-io/nats.go"
    "github.com/nats-io/nats.go/jetstream"

    goKitLog "github.com/go-kit/log"
    "github.com/nrfta/go-outbox"
    "github.com/nrfta/go-outbox/mb/nats"
    "github.com/nrfta/go-outbox/store/pg"
)

func NewNatsOutbox(db *sql.DB) (outbox.Outbox[*nats.Msg], error) {
    nc, err := nats.Connect(nats.DefaultURL)
    if err != nil {
        log.Fatal(err)
    }

    // setup streams (create or update)
    js, err := jetstream.New(nc)
    if err != nil {
        return nil, err
    }

    messageBroker, err := mbNats.NewJetstream(js)
    if err != nil {
        return nil, err
    }

    outboxStore, err := outboxPg.NewStore(
        db,
        "your_db_conn_string_here",
        goKitLog.NewJSONLogger(goKitLog.NewSyncWriter(os.Stderr)),
        pg.WithTableName("outbox"),
    )
    if err != nil {
        return nil, err
    }

    outbox := outbox.New[*nats.Msg](
        outboxStore,
        messageBroker,
        logDeadLetterQueue{},
        outbox.WithSlogLogger[*nats.Msg](slog.Default()),
    )

    return outbox, nil
}
```

## Usage

Here is an example of how to use the outbox to send a message:

```go
package main

import (
    "context"
    "database/sql"
    "log"

    _ "github.com/lib/pq" // PostgreSQL driver
    "github.com/nats-io/nats.go"
    "github.com/nrfta/go-outbox"
)

func main() {
    // Initialize your database connection (replace with your connection details)
    db, err := sql.Open("postgres", "your_db_conn_string_here")
    if err != nil {
        log.Fatal(err)
    }

    // Initialize the outbox
    outbox, err := NewNatsOutbox(db)
    if err != nil {
        log.Fatal(err)
    }

    // Create a message
    msg := &nats.Msg{
        Subject: "hello",
        Data:    []byte("hello"),
    }

    // Send the message within a transaction
    tx, err := db.Begin()
    if err != nil {
        log.Fatal(err)
    }

    if err := outbox.SendTx(context.Background(), tx, msg); err != nil {
        log.Fatal(err)
    }

    if err := tx.Commit(); err != nil {
        log.Fatal(err)
    }
}

```
