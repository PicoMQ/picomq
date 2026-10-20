# Go client

The native Go SDK speaks PicoMQ's Pico and Durable Streams HTTP protocols, without cgo or a Rust runtime.

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

`NewDurableStreams` builds a Durable Streams client, and `Connect` returns the protocol-neutral `Client` interface. Constructors do no network I/O. Every call takes a `context.Context`, and cancelling it interrupts both requests and retry delays.

## Partitions

A topic created over Kafka can have several partitions. `stream.Partition(i)` addresses one, with the same `Head`, `Append`, `Read`, `Records`, and `Subscribe` calls, plus `Trim`, `AppendAs`, and `NewProducer` on Pico.

```go
partition := client.Stream("/orders").Partition(2)
page, err := partition.Read(ctx, client.Beginning(), picomq.ReadOptions{})
```

## Reads and subscriptions

Positions are protocol-specific opaque strings. Always continue from the `Next` position a call returned.

- `Read` fetches one page.
- `Records` iterates over records across pages. `RecordsOptions.Live` keeps long polling after it catches up.
- `Subscribe` opens an SSE stream of data and control events. It reconnects by default, resumes with `Last-Event-ID`, and stops when its context is cancelled or `Close` is called.

## Producers

`NewProducer` on a Pico stream batches records, bounds buffered bytes, and retries safely under a producer id, epoch, and sequence.

```go
producer := client.Stream("/orders").NewProducer("writer-1", nil)
seq, err := producer.SendDurable(ctx, picomq.AppendRecord{Body: []byte("x")})
```

`Send` returns a pending acknowledgement, and `SendDurable` waits for the assigned position. A terminal batch failure poisons the producer, and recovery takes a new producer with a higher epoch. For one-off idempotent appends, `AppendAs` sends records under an explicit `ProducerRef`. Plain appends are never retried, because they carry no deduplication key.

## Consumer groups

A group member joins, heartbeats in the background, and follows rebalances. See [Consumer groups](/docs/design/groups) for how assignment works.

```go
member, err := client.NewGroupMember(ctx, "billing", []string{"/orders", "/payments"}, nil)
if err != nil {
    log.Fatal(err)
}
defer member.Leave(ctx)

for _, topic := range member.Assignment().Streams {
    // read topic, then commit progress
    _ = member.Commit(ctx, picomq.Offsets{topic: {Position: 42}})
}
```

`Assignments` delivers each new assignment after a rebalance, and `Err` reports a member that could not rejoin. The lower-level calls are on `PicoClient`: `JoinGroup`, `Heartbeat`, `GroupAssignment`, `LeaveGroup`, `CommitOffsets`, `FetchOffsets`, `DescribeGroup`, `ListGroups`, and `DeleteGroup`.

## Configuration

| Option | Effect |
| --- | --- |
| `WithToken` | Sends a bearer token on every request, including each redirect hop |
| `WithRetryPolicy` | Retries read-shaped calls. `NoRetries` and `RetryAttempts` build common policies |
| `WithH2C` | Speaks cleartext HTTP/2 for multiplexed appends |
| `WithHTTPClient` | Uses your own `*http.Client` |

The client follows ownership redirects itself, keeping the credential and request body, see the [HTTP API conventions](/docs/api#conventions).

## Errors

Errors unwrap to `*picomq.ClientError`, with a kind, the HTTP status, and the server's code. `picomq.IsKind` classifies them. Operations a protocol has no equivalent for, such as `List`, `Trim`, and groups over Durable Streams, return the `ErrorUnsupported` kind.
