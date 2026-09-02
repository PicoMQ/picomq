package picomq

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = ginkgo.Describe("Pico producer", func() {
	ginkgo.It("batches records and resolves durable positions", func() {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			Expect(r.Header.Get("Pico-Producer-Id")).To(Equal("writer-1"))
			Expect(r.Header.Get("Pico-Producer-Epoch")).To(Equal("3"))
			Expect(r.Header.Get("Pico-Producer-Seq")).To(Equal("0"))
			data, err := body(&http.Response{Body: r.Body}, maxResponseBody)
			Expect(err).NotTo(HaveOccurred())
			Expect(binary.BigEndian.Uint32(data[1:5])).To(Equal(uint32(2)))
			w.Header().Set("Pico-Start-Seq", "5")
			w.Header().Set("Pico-Next-Seq", "7")
		}))
		defer server.Close()
		client, err := NewPico(server.URL)
		Expect(err).NotTo(HaveOccurred())
		config := DefaultProducerConfig()
		config.Epoch = 3
		config.Linger = 25 * time.Millisecond
		producer := client.Stream("orders").NewProducer("writer-1", &config)
		first, err := producer.Send(context.Background(), AppendRecord{Body: []byte("a")})
		Expect(err).NotTo(HaveOccurred())
		second, err := producer.Send(context.Background(), AppendRecord{Body: []byte("b")})
		Expect(err).NotTo(HaveOccurred())
		seq, err := first.Await(context.Background())
		Expect(err).NotTo(HaveOccurred())
		Expect(seq).To(Equal(uint64(5)))
		seq, err = second.Await(context.Background())
		Expect(err).NotTo(HaveOccurred())
		Expect(seq).To(Equal(uint64(6)))
		Expect(producer.Close(context.Background())).To(Succeed())
	})

	ginkgo.It("snapshots the body and headers before returning from Send", func() {
		received := make(chan AppendRecord, 1)
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			data, err := body(&http.Response{Body: r.Body}, maxResponseBody)
			Expect(err).NotTo(HaveOccurred())
			records, err := decodeAppendBatchForTest(data)
			Expect(err).NotTo(HaveOccurred())
			Expect(records).To(HaveLen(1))
			received <- records[0]
			w.Header().Set("Pico-Start-Seq", "0")
			w.Header().Set("Pico-Next-Seq", "1")
		}))
		defer server.Close()

		client, err := NewPico(server.URL)
		Expect(err).NotTo(HaveOccurred())
		config := DefaultProducerConfig()
		config.Linger = 50 * time.Millisecond
		producer := client.Stream("orders").NewProducer("writer-1", &config)

		body := []byte("original-body")
		key := []byte("original-key")
		headers := map[string][]byte{"source": []byte("original")}
		pending, err := producer.Send(context.Background(), AppendRecord{Body: body, Key: key, Headers: headers})
		Expect(err).NotTo(HaveOccurred())
		copy(body, []byte("mutated-body!"))
		copy(key, []byte("mutated-key!"))
		headers["source"][0] = 'X'
		headers["added"] = []byte("later")

		_, err = pending.Await(context.Background())
		Expect(err).NotTo(HaveOccurred())
		Expect(<-received).To(Equal(AppendRecord{Body: []byte("original-body"), Key: []byte("original-key"), Headers: map[string][]byte{"source": []byte("original")}}))
		Expect(producer.Close(context.Background())).To(Succeed())
	})

	ginkgo.It("allows Await to be retried after its context is canceled", func() {
		result := make(chan pendingResult, 1)
		pending := &Pending{result: result}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := pending.Await(ctx)
		Expect(err).To(MatchError(context.Canceled))
		result <- pendingResult{seq: 12}
		seq, err := pending.Await(context.Background())
		Expect(err).NotTo(HaveOccurred())
		Expect(seq).To(Equal(uint64(12)))
	})

	ginkgo.It("accounts for keys and headers in the buffer budget", func() {
		client, err := NewPico("http://127.0.0.1:4437")
		Expect(err).NotTo(HaveOccurred())
		config := DefaultProducerConfig()
		config.MaxBufferedBytes = 5
		producer := client.Stream("orders").NewProducer("writer", &config)
		_, err = producer.Send(context.Background(), AppendRecord{Key: []byte("key"), Headers: map[string][]byte{"h": []byte("xx")}})
		Expect(err).To(MatchError(ContainSubstring("exceeds producer budget")))
		Expect(producer.Close(context.Background())).To(Succeed())
	})

	ginkgo.It("rejects records larger than the batch limit", func() {
		client, err := NewPico("http://127.0.0.1:4437")
		Expect(err).NotTo(HaveOccurred())
		config := DefaultProducerConfig()
		config.MaxBatchBytes = 5
		producer := client.Stream("orders").NewProducer("writer", &config)
		_, err = producer.Send(context.Background(), AppendRecord{Body: []byte("123456")})
		Expect(err).To(MatchError(ContainSubstring("exceeds batch limit")))
		Expect(producer.Close(context.Background())).To(Succeed())
	})

	ginkgo.It("carries a record that would cross the batch limit", func() {
		var calls atomic.Int32
		counts := make(chan uint32, 2)
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			data, err := body(&http.Response{Body: r.Body}, maxResponseBody)
			Expect(err).NotTo(HaveOccurred())
			counts <- binary.BigEndian.Uint32(data[1:5])
			call := calls.Add(1)
			w.Header().Set("Pico-Start-Seq", strconv.Itoa(int(call-1)))
			w.Header().Set("Pico-Next-Seq", strconv.Itoa(int(call)))
		}))
		defer server.Close()
		client, err := NewPico(server.URL)
		Expect(err).NotTo(HaveOccurred())
		config := DefaultProducerConfig()
		config.Linger = 20 * time.Millisecond
		config.MaxBatchBytes = 5
		producer := client.Stream("orders").NewProducer("writer", &config)
		first, err := producer.Send(context.Background(), AppendRecord{Body: []byte("123")})
		Expect(err).NotTo(HaveOccurred())
		second, err := producer.Send(context.Background(), AppendRecord{Body: []byte("456")})
		Expect(err).NotTo(HaveOccurred())
		_, err = first.Await(context.Background())
		Expect(err).NotTo(HaveOccurred())
		_, err = second.Await(context.Background())
		Expect(err).NotTo(HaveOccurred())
		Expect(producer.Close(context.Background())).To(Succeed())
		Expect(<-counts).To(Equal(uint32(1)))
		Expect(<-counts).To(Equal(uint32(1)))
	})

	ginkgo.It("reports duplicate positions as unknown", func() {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Pico-Next-Seq", "99")
		}))
		defer server.Close()
		client, err := NewPico(server.URL)
		Expect(err).NotTo(HaveOccurred())
		producer := &Producer{client: client, name: "orders", id: "writer", config: DefaultProducerConfig()}
		_, err = producer.sendBatch([]AppendRecord{{Body: []byte("value")}}, 0)
		var clientErr *ClientError
		Expect(errors.As(err, &clientErr)).To(BeTrue())
		Expect(clientErr.Code).To(Equal("duplicate_position_unknown"))
	})

	ginkgo.It("retries sequence gaps", func() {
		var calls atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if calls.Add(1) == 1 {
				w.WriteHeader(http.StatusConflict)
				_, _ = w.Write([]byte(`{"error":"sequence_gap"}`))
				return
			}
			w.Header().Set("Pico-Start-Seq", "7")
			w.Header().Set("Pico-Next-Seq", "8")
		}))
		defer server.Close()
		client, err := NewPico(server.URL)
		Expect(err).NotTo(HaveOccurred())
		config := DefaultProducerConfig()
		config.Retry = RetryPolicy{MaxAttempts: 2}
		producer := &Producer{client: client, name: "orders", id: "writer", config: config}
		start, err := producer.sendBatch([]AppendRecord{{Body: []byte("value")}}, 0)
		Expect(err).NotTo(HaveOccurred())
		Expect(start).To(Equal(uint64(7)))
		Expect(calls.Load()).To(Equal(int32(2)))
	})
})

func decodeAppendBatchForTest(data []byte) ([]AppendRecord, error) {
	r := bytes.NewReader(data)
	if _, err := r.ReadByte(); err != nil {
		return nil, err
	}
	count, err := readU32(r)
	if err != nil {
		return nil, err
	}
	records := make([]AppendRecord, 0, count)
	for i := uint32(0); i < count; i++ {
		key, err := readKey(r)
		if err != nil {
			return nil, err
		}
		headers, err := readHeaders(r)
		if err != nil {
			return nil, err
		}
		value, err := readBytes(r)
		if err != nil {
			return nil, err
		}
		records = append(records, AppendRecord{Body: value, Key: key, Headers: headers})
	}
	return records, nil
}
