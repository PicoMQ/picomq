package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"time"

	picomq "github.com/PicoMQ/picomq/client/go"
)

const contentType = "application/octet-stream"

type result struct {
	label   string
	elapsed time.Duration
	records int
	bytes   int64
}

func main() {
	endpoint := flag.String("endpoint", envOr("PICO_ENDPOINT", "http://127.0.0.1:4437"), "Pico endpoint")
	sequentialCount := flag.Int("sequential", 300, "number of sequential appends")
	batchCount := flag.Int("batch", 5000, "number of small producer records")
	largeCount := flag.Int("large", 1000, "number of large producer records")
	flag.Parse()

	ctx := context.Background()
	client, err := picomq.NewPico(*endpoint)
	if err != nil {
		log.Fatal(err)
	}
	defer client.CloseIdleConnections()

	base := "/bench-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	defer cleanup(client, base)

	fmt.Printf("endpoint %s\n\n", *endpoint)
	run(sequentialAppends(ctx, client, base+"/seq", *sequentialCount, 128))
	run(producerRun(ctx, client, base+"/batch", "batch", *batchCount, 128))
	run(producerRun(ctx, client, base+"/large", "large", *largeCount, 8192))
	run(readBack(ctx, client, base+"/batch", "batch", *batchCount))
	run(readBack(ctx, client, base+"/large", "large", *largeCount))
}

func sequentialAppends(ctx context.Context, client *picomq.PicoClient, name string, count, size int) (result, error) {
	stream := client.Stream(name)
	if _, err := stream.Create(ctx, contentType, 0); err != nil {
		return result{}, err
	}
	body := payload(size, "seq")
	started := time.Now()
	for i := 0; i < count; i++ {
		if _, err := stream.Append(ctx, picomq.AppendRecord{Body: body}); err != nil {
			return result{}, err
		}
	}
	return result{label: fmt.Sprintf("sequential append (%dB, 1 rec/req)", size), elapsed: time.Since(started), records: count, bytes: int64(count * size)}, nil
}

func producerRun(ctx context.Context, client *picomq.PicoClient, name, label string, count, size int) (result, error) {
	stream := client.Stream(name)
	if _, err := stream.Create(ctx, contentType, 0); err != nil {
		return result{}, err
	}
	config := picomq.DefaultProducerConfig()
	producer := stream.NewProducer("bench-"+label, &config)
	defer producer.Close(context.Background())
	body := payload(size, label)
	started := time.Now()
	pendings := make([]*picomq.Pending, count)
	for i := 0; i < count; i++ {
		pending, err := producer.Send(ctx, picomq.AppendRecord{Body: body})
		if err != nil {
			return result{}, err
		}
		pendings[i] = pending
	}
	for i, pending := range pendings {
		seq, err := pending.Await(ctx)
		if err != nil {
			return result{}, err
		}
		if seq != uint64(i) {
			return result{}, fmt.Errorf("ordering violated at %d: got %d", i, seq)
		}
	}
	if err := producer.Close(ctx); err != nil {
		return result{}, err
	}
	elapsed := time.Since(started)
	info, err := stream.Head(ctx)
	if err != nil {
		return result{}, err
	}
	if info == nil || info.Next != strconv.Itoa(count) {
		return result{}, fmt.Errorf("expected next %d, got %v", count, info)
	}
	return result{label: fmt.Sprintf("producer %s (linger %s)", label, config.Linger), elapsed: elapsed, records: count, bytes: int64(count * size)}, nil
}

func readBack(ctx context.Context, client *picomq.PicoClient, name, label string, expected int) (result, error) {
	iterator := client.Stream(name).Records(picomq.RecordsOptions{From: client.Beginning(), Limits: picomq.ReadLimits{Count: 1000}})
	started := time.Now()
	var records int
	var bytes int64
	for {
		record, err := iterator.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return result{}, err
		}
		records++
		bytes += int64(len(record.Body))
	}
	if records != expected {
		return result{}, fmt.Errorf("expected %d records, read %d", expected, records)
	}
	return result{label: fmt.Sprintf("read back %s (pages of 1000)", label), elapsed: time.Since(started), records: records, bytes: bytes}, nil
}

func payload(size int, tag string) []byte {
	body := make([]byte, size)
	copy(body, tag)
	return body
}

func run(value result, err error) {
	if err != nil {
		log.Fatal(err)
	}
	seconds := value.elapsed.Seconds()
	fmt.Printf("%-46s %7d rec  %6.2fs  %8s rec/s  %7.1f MB/s\n", value.label, value.records, seconds, formatRate(float64(value.records)/seconds), float64(value.bytes)/seconds/(1024*1024))
}

func formatRate(value float64) string {
	if value >= 1000 {
		return fmt.Sprintf("%.1fk", value/1000)
	}
	return fmt.Sprintf("%.0f", value)
}

func cleanup(client *picomq.PicoClient, base string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	listing, err := client.List(ctx, base+"/", 100)
	if err != nil {
		log.Printf("cleanup: %v", err)
		return
	}
	for _, info := range listing.Streams {
		if _, err := client.Delete(ctx, info.Name); err != nil {
			log.Printf("cleanup %s: %v", info.Name, err)
		}
	}
	fmt.Printf("\ncleaned up %d bench streams\n", len(listing.Streams))
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
