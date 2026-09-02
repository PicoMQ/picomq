package picomq

import (
	"bytes"
	"context"
	"encoding/binary"
	"net/http"
	"net/http/httptest"
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
		headers := map[string]string{"source": "original"}
		pending, err := producer.Send(context.Background(), AppendRecord{Body: body, Headers: headers})
		Expect(err).NotTo(HaveOccurred())
		copy(body, []byte("mutated-body!"))
		headers["source"] = "mutated"
		headers["added"] = "later"

		_, err = pending.Await(context.Background())
		Expect(err).NotTo(HaveOccurred())
		Expect(<-received).To(Equal(AppendRecord{Body: []byte("original-body"), Headers: map[string]string{"source": "original"}}))
		Expect(producer.Close(context.Background())).To(Succeed())
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
		headers, err := readHeaders(r)
		if err != nil {
			return nil, err
		}
		value, err := readBytes(r)
		if err != nil {
			return nil, err
		}
		records = append(records, AppendRecord{Body: value, Headers: headers})
	}
	return records, nil
}
