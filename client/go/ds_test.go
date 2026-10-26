package picomq

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = ginkgo.Describe("Durable Streams client", func() {
	ginkgo.It("uses opaque offsets and raw message bodies", func() {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPost {
				w.Header().Set("Stream-Next-Offset", "offset-1")
				w.WriteHeader(http.StatusNoContent)
				return
			}
			Expect(r.URL.Query().Get("offset")).To(Equal("offset-1"))
			w.Header().Set("Stream-Next-Offset", "offset-2")
			w.Header().Set("Stream-Up-To-Date", "true")
			_, _ = w.Write([]byte("message"))
		}))
		defer server.Close()
		client, err := NewDurableStreams(server.URL)
		Expect(err).NotTo(HaveOccurred())
		ack, err := client.Append(context.Background(), "events", []AppendRecord{{Body: []byte("message")}})
		Expect(err).NotTo(HaveOccurred())
		Expect(ack.Next).To(Equal("offset-1"))
		page, err := client.Read(context.Background(), "events", ack.Next, ReadOptions{})
		Expect(err).NotTo(HaveOccurred())
		Expect(page.Next).To(Equal("offset-2"))
		Expect(page.Records[0].Body).To(Equal([]byte("message")))
	})

	ginkgo.It("sends the partition from partition handles", func() {
		var mu sync.Mutex
		var requests []string
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			requests = append(requests, r.Method+"?"+strings.Join(r.URL.Query()["partition"], ","))
			mu.Unlock()

			switch {
			case r.Method == http.MethodHead:
				w.Header().Set("Stream-Partitions", "2")
				if r.URL.Query().Has("partition") {
					w.Header().Set("Stream-Next-Offset", "offset-1")
				}
			case r.Method == http.MethodPost:
				w.Header().Set("Stream-Next-Offset", "offset-1")
				w.WriteHeader(http.StatusNoContent)
			case r.URL.Query().Get("live") == "sse":
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, "event: control\ndata: {\"streamNextOffset\":\"offset-1\",\"upToDate\":true,\"streamClosed\":true}\n\n")
			default:
				w.Header().Set("Stream-Next-Offset", "offset-1")
				w.Header().Set("Stream-Up-To-Date", "true")
			}
		}))
		defer server.Close()

		client, err := NewDurableStreams(server.URL)
		Expect(err).NotTo(HaveOccurred())
		ctx := context.Background()
		partition := client.Stream("events").Partition(1)

		info, err := client.Head(ctx, "events")
		Expect(err).NotTo(HaveOccurred())
		Expect(*info).To(Equal(StreamInfo{Name: "events", Partitions: 2}))
		info, err = partition.Head(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(*info).To(Equal(StreamInfo{Name: "events", Start: "-1", Next: "offset-1", Partitions: 2}))

		_, err = partition.Append(ctx, AppendRecord{Body: []byte("message")})
		Expect(err).NotTo(HaveOccurred())
		_, err = partition.Read(ctx, "-1", ReadOptions{})
		Expect(err).NotTo(HaveOccurred())
		subscription := partition.Subscribe(ctx, "-1", SubscribeOptions{DisableReconnect: true})
		event, err := subscription.Next()
		Expect(err).NotTo(HaveOccurred())
		Expect(event.Closed).To(BeTrue())
		_, err = partition.Records(RecordsOptions{}).Next(ctx)
		Expect(err).To(MatchError(io.EOF))

		mu.Lock()
		defer mu.Unlock()
		Expect(requests).To(Equal([]string{"HEAD?", "HEAD?1", "POST?1", "GET?1", "GET?1", "GET?1"}))
	})

	ginkgo.It("reports unsupported union operations as structured errors", func() {
		client, err := NewDurableStreams("http://127.0.0.1:4437")
		Expect(err).NotTo(HaveOccurred())
		_, err = client.List(context.Background(), "", 0, "")
		Expect(IsKind(err, ErrorUnsupported)).To(BeTrue())
		var clientError *ClientError
		Expect(errors.As(err, &clientError)).To(BeTrue())
		Expect(clientError.Code).To(Equal("unsupported"))
	})

	ginkgo.It("rounds positive TTLs up to seconds", func() {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			Expect(r.Header.Get("Stream-TTL")).To(Equal("1"))
			w.WriteHeader(http.StatusCreated)
		}))
		defer server.Close()
		client, err := NewDurableStreams(server.URL)
		Expect(err).NotTo(HaveOccurred())
		_, err = client.Create(context.Background(), "events", CreateOptions{ContentType: "application/octet-stream", TTL: time.Nanosecond})
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.It("rejects record keys as unsupported", func() {
		client, err := NewDurableStreams("http://127.0.0.1:4437")
		Expect(err).NotTo(HaveOccurred())
		_, err = client.Append(context.Background(), "events", []AppendRecord{{Key: []byte("key"), Body: []byte("value")}})
		Expect(IsKind(err, ErrorUnsupported)).To(BeTrue())
	})

	ginkgo.It("rejects null record bodies as unsupported", func() {
		client, err := NewDurableStreams("http://127.0.0.1:4437")
		Expect(err).NotTo(HaveOccurred())
		_, err = client.Append(context.Background(), "events", []AppendRecord{{Body: nil}})
		Expect(IsKind(err, ErrorUnsupported)).To(BeTrue())
	})
})
