# Go client

The native Go SDK supports PicoMQ's Pico and Durable Streams HTTP protocols without cgo or a Rust runtime.

## Install

```sh
go get github.com/PicoMQ/picomq/client/go
```

## Usage

```go
package main

import (
    "context"
    "fmt"
    "log"

    picomq "github.com/PicoMQ/picomq/client/go"
)

func main() {
    ctx := context.Background()
    client, err := picomq.NewPico("http://127.0.0.1:4437")
    if err != nil {
        log.Fatal(err)
    }
    stream := client.Stream("/orders/1042")
    if _, err := stream.Create(ctx, "application/json", 0); err != nil {
        log.Fatal(err)
    }
    ack, err := stream.Append(ctx, picomq.AppendRecord{Body: []byte(`{"item":"widget"}`)})
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("appended at %s\n", ack.Start)

    page, err := stream.Read(ctx, client.Beginning(), picomq.ReadOptions{})
    if err != nil {
        log.Fatal(err)
    }
    for _, record := range page.Records {
        fmt.Printf("%s: %s\n", record.Position, record.Body)
    }
}
```

Construct a Durable Streams client with `NewDurableStreams`. Constructors do not perform network I/O.

## Reads and live records

Positions are protocol-specific opaque strings. Always continue from the `Next` position returned by PicoMQ.

`Read` fetches one page. `Records` builds a record iterator over page reads. Set `RecordsOptions.Live` to use long polling after the iterator catches up. Cancel the context passed to `Next` to interrupt a pending read.

## SSE subscriptions

`Subscribe` opens the SSE endpoint and returns data and control events. Subscriptions reconnect by default, send `Last-Event-ID` when resuming, and stop when their lifetime context is canceled or `Close` is called.

Data events contain decoded records. Control events expose the next position, caught-up state, and stream-closed state. Call `Close` when abandoning a subscription.

## Producers

Pico streams provide `NewProducer`. Producers batch records, bound buffered bytes, sequence batches, and safely retry using producer identity, epoch, and sequence. `Send` returns a pending acknowledgement; `SendDurable` waits directly for the assigned record position. A terminal batch failure poisons the producer, and recovery requires a new producer with a higher epoch.

Plain appends are never retried automatically because they do not carry a deduplication key.

## Authentication and retries

Use `WithToken` to attach a bearer token. Ownership redirects are followed explicitly so credentials and request bodies are preserved across PicoMQ nodes.

Use `WithRetryPolicy` to configure retries for safe, read-shaped operations. Context cancellation interrupts both network requests and retry delays.

Errors can be inspected with `errors.As` into `*picomq.ClientError` or classified with `picomq.IsKind`.

## Protocol differences

The protocol-neutral `Client` interface is the union of the shared client surface. Operations without a Durable Streams equivalent, such as `List`, return a structured `unsupported` error. Trimming, record headers, and identified producers are Pico-specific stream extensions. Durable Streams appends exactly one raw record per request. Pico starts at `"0"`; Durable Streams begins at `"-1"` and also supports the `"now"` position.
