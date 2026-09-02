# PicoMQ Go client

Native Go client for PicoMQ's Pico and Durable Streams HTTP protocols.

## Install

```sh
go get github.com/PicoMQ/picomq/client/go
```

## Quick start

```go
package main

import (
    "context"
    "log"

    picomq "github.com/PicoMQ/picomq/client/go"
)

func main() {
	ctx := context.Background()
	client, err := picomq.NewPico("http://127.0.0.1:4437")
	if err != nil {
		log.Fatal(err)
	}
	stream := client.Stream("/orders")
	_, err = stream.Create(ctx, "application/json", 0)
	if err != nil {
		log.Fatal(err)
	}
	_, err = stream.Append(ctx, picomq.AppendRecord{Body: []byte(`{"id": 42}`)})
	if err != nil {
		log.Fatal(err)
	}
}
```

All network operations accept `context.Context`. Positions are opaque strings; continue reads using the `Next` value returned by the server.

`Connect` returns the union `Client` interface for applications that select a protocol dynamically. Unsupported protocol operations return a structured `ClientError` with kind `ErrorUnsupported`.

Use `Records` for record-only finite or long-poll consumption. Use `Subscribe` when SSE data and control events, reconnection, and `Last-Event-ID` resumption are required.

## Development

Tests use Ginkgo v2 and Gomega:

```sh
go test ./...
go test -race ./...
```

Run the live suite against a Pico server:

```sh
PICOMQ_INTEGRATION=1 PICOMQ_ENDPOINT=http://127.0.0.1:4437 go test ./...
```

For a listener running the Durable Streams protocol:

```sh
PICOMQ_DS_INTEGRATION=1 PICOMQ_DS_ENDPOINT=http://127.0.0.1:4437 go test ./...
```
