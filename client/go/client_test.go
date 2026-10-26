package picomq

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = ginkgo.Describe("Pico client", func() {
	ginkgo.It("creates, appends, and reads with the Pico wire contract", func() {
		var calls atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch calls.Add(1) {
			case 1:
				Expect(r.Method).To(Equal(http.MethodPut))
				Expect(r.URL.Path).To(Equal("/orders"))
				w.WriteHeader(http.StatusCreated)
			case 2:
				Expect(r.Method).To(Equal(http.MethodPost))
				Expect(r.Header.Get("Content-Type")).To(Equal("application/vnd.picomq.batch"))
				w.Header().Set("Pico-Start-Seq", "0")
				w.Header().Set("Pico-Next-Seq", "1")
			case 3:
				Expect(r.URL.Query().Get("format")).To(Equal("binary"))
				Expect(r.URL.Query().Get("seq")).To(Equal("0"))
				w.Header().Set("Pico-Next-Seq", "1")
				w.Header().Set("Pico-Up-To-Date", "true")
				var payload bytes.Buffer
				payload.WriteByte(1)
				_ = binary.Write(&payload, binary.BigEndian, uint32(1))
				_ = binary.Write(&payload, binary.BigEndian, uint64(0))
				_ = binary.Write(&payload, binary.BigEndian, int64(0))
				_ = binary.Write(&payload, binary.BigEndian, int32(-1))
				_ = binary.Write(&payload, binary.BigEndian, uint32(0))
				_ = binary.Write(&payload, binary.BigEndian, uint32(1))
				payload.WriteByte('x')
				_, _ = w.Write(payload.Bytes())
			}
		}))
		defer server.Close()
		client, err := NewPico(server.URL)
		Expect(err).NotTo(HaveOccurred())
		created, err := client.Create(context.Background(), "/orders", CreateOptions{ContentType: "application/json"})
		Expect(err).NotTo(HaveOccurred())
		Expect(created).To(BeTrue())
		ack, err := client.Append(context.Background(), "/orders", []AppendRecord{{Body: []byte("x")}})
		Expect(err).NotTo(HaveOccurred())
		Expect(ack.Next).To(Equal("1"))
		page, err := client.Read(context.Background(), "/orders", "0", ReadOptions{})
		Expect(err).NotTo(HaveOccurred())
		Expect(page.Records).To(HaveLen(1))
		Expect(page.Records[0].Body).To(Equal([]byte("x")))
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
				w.Header().Set("Pico-Partitions", "3")
				if r.URL.Query().Has("partition") {
					w.Header().Set("Pico-Start-Seq", "0")
					w.Header().Set("Pico-Next-Seq", "4")
				}
			case r.Method == http.MethodPost && r.Header.Get("Pico-Trim-Seq") != "":
				w.Header().Set("Pico-Start-Seq", "2")
			case r.Method == http.MethodPost:
				w.Header().Set("Pico-Start-Seq", "0")
				w.Header().Set("Pico-Next-Seq", "1")
			case r.URL.Query().Get("live") == "sse":
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, "event: control\ndata: {\"next_seq\":4,\"up_to_date\":true,\"closed\":true}\n\n")
			default:
				w.Header().Set("Pico-Next-Seq", "4")
				w.Header().Set("Pico-Up-To-Date", "true")
			}
		}))
		defer server.Close()

		client, err := NewPico(server.URL)
		Expect(err).NotTo(HaveOccurred())
		ctx := context.Background()
		partition := client.Stream("orders").Partition(2)

		info, err := client.Head(ctx, "orders")
		Expect(err).NotTo(HaveOccurred())
		Expect(*info).To(Equal(StreamInfo{Name: "orders", Partitions: 3}))
		info, err = partition.Head(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(info.Partitions).To(Equal(uint32(3)))
		Expect(info.Start).To(Equal("0"))
		Expect(info.Next).To(Equal("4"))

		_, err = partition.Append(ctx, AppendRecord{Body: []byte("a")})
		Expect(err).NotTo(HaveOccurred())
		_, err = partition.AppendAs(ctx, []AppendRecord{{Body: []byte("b")}}, ProducerRef{ID: "writer", Seq: 0})
		Expect(err).NotTo(HaveOccurred())
		_, err = partition.Read(ctx, "0", ReadOptions{})
		Expect(err).NotTo(HaveOccurred())
		start, err := partition.Trim(ctx, 2)
		Expect(err).NotTo(HaveOccurred())
		Expect(start).To(Equal("2"))

		subscription := partition.Subscribe(ctx, "0", SubscribeOptions{DisableReconnect: true})
		event, err := subscription.Next()
		Expect(err).NotTo(HaveOccurred())
		Expect(event.Closed).To(BeTrue())
		_, err = partition.Records(RecordsOptions{}).Next(ctx)
		Expect(err).To(MatchError(io.EOF))

		producer := partition.NewProducer("writer", nil)
		_, err = producer.SendDurable(ctx, AppendRecord{Body: []byte("c")})
		Expect(err).NotTo(HaveOccurred())
		Expect(producer.Close(ctx)).To(Succeed())

		mu.Lock()
		defer mu.Unlock()
		Expect(requests).To(Equal([]string{"HEAD?", "HEAD?2", "POST?2", "POST?2", "GET?2", "POST?2", "GET?2", "GET?2", "POST?2"}))
	})

	ginkgo.It("lists items and returns the next cursor", func() {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.WriteString(w, `{"items":[{"name":"/single","content_type":"application/octet-stream","partitions":1},{"name":"/multi","content_type":"application/octet-stream","partitions":4}],"next":"opaque"}`)
		}))
		defer server.Close()

		client, err := NewPico(server.URL)
		Expect(err).NotTo(HaveOccurred())
		listing, err := client.List(context.Background(), "/", 0, "")
		Expect(err).NotTo(HaveOccurred())
		Expect(listing.Streams).To(Equal([]StreamInfo{
			{Name: "/single", ContentType: "application/octet-stream", Partitions: 1},
			{Name: "/multi", ContentType: "application/octet-stream", Partitions: 4},
		}))
		Expect(listing.Next).To(Equal("opaque"))
	})

	ginkgo.It("authenticates requests without explicit headers", func() {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			Expect(r.Header.Get("Authorization")).To(Equal("Bearer secret"))
		}))
		defer server.Close()
		client, err := NewPico(server.URL, WithToken("secret"))
		Expect(err).NotTo(HaveOccurred())
		_, err = client.Head(context.Background(), "orders")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.It("rounds positive TTLs up to seconds", func() {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			Expect(r.Header.Get("Pico-TTL")).To(Equal("2"))
			w.WriteHeader(http.StatusCreated)
		}))
		defer server.Close()
		client, err := NewPico(server.URL)
		Expect(err).NotTo(HaveOccurred())
		_, err = client.Create(context.Background(), "orders", CreateOptions{ContentType: "application/octet-stream", TTL: time.Second + time.Nanosecond})
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.It("reattaches credentials across ownership redirects", func() {
		target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			Expect(r.Header.Get("Authorization")).To(Equal("Bearer secret"))
			w.WriteHeader(http.StatusCreated)
		}))
		defer target.Close()
		origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, target.URL+r.URL.Path, http.StatusTemporaryRedirect)
		}))
		defer origin.Close()
		client, err := NewPico(origin.URL, WithToken("secret"))
		Expect(err).NotTo(HaveOccurred())
		created, err := client.Create(context.Background(), "redirected", CreateOptions{ContentType: "application/octet-stream"})
		Expect(err).NotTo(HaveOccurred())
		Expect(created).To(BeTrue())
	})

	ginkgo.It("goes straight to a learned owner until it is gone", func() {
		owner := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodDelete {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			w.Header().Set("Pico-Next-Seq", "7")
		}))
		defer owner.Close()
		var redirects atomic.Int32
		var target atomic.Value
		target.Store(owner.URL)
		entry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			redirects.Add(1)
			http.Redirect(w, r, target.Load().(string)+r.URL.Path, http.StatusTemporaryRedirect)
		}))
		defer entry.Close()
		ctx := context.Background()

		client, err := NewPico(entry.URL)
		Expect(err).NotTo(HaveOccurred())
		for i := 0; i < 3; i++ {
			info, err := client.Head(ctx, "orders")
			Expect(err).NotTo(HaveOccurred())
			Expect(info.Next).To(Equal("7"))
		}
		Expect(redirects.Load()).To(BeEquivalentTo(1))

		deleted, err := client.Delete(ctx, "orders")
		Expect(err).NotTo(HaveOccurred())
		Expect(deleted).To(BeTrue())
		_, err = client.Head(ctx, "orders")
		Expect(err).NotTo(HaveOccurred())
		Expect(redirects.Load()).To(BeEquivalentTo(2))

		gone := httptest.NewServer(http.NotFoundHandler())
		gone.Close()
		target.Store(gone.URL)
		fresh, err := NewPico(entry.URL)
		Expect(err).NotTo(HaveOccurred())
		_, err = fresh.Head(ctx, "orders")
		Expect(err).To(HaveOccurred())
		target.Store(owner.URL)
		_, err = fresh.Head(ctx, "orders")
		Expect(err).NotTo(HaveOccurred())
		Expect(redirects.Load()).To(BeEquivalentTo(4))
	})

	ginkgo.It("keeps an owner's path prefix for learned routes", func() {
		owner := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/node2/orders" {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("Pico-Next-Seq", "7")
		}))
		defer owner.Close()
		var redirects atomic.Int32
		entry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			redirects.Add(1)
			path := strings.TrimPrefix(r.URL.Path, "/node1")
			http.Redirect(w, r, owner.URL+"/node2"+path, http.StatusTemporaryRedirect)
		}))
		defer entry.Close()

		client, err := NewPico(entry.URL + "/node1")
		Expect(err).NotTo(HaveOccurred())
		for i := 0; i < 3; i++ {
			info, err := client.Head(context.Background(), "orders")
			Expect(err).NotTo(HaveOccurred())
			Expect(info.Next).To(Equal("7"))
		}
		Expect(redirects.Load()).To(BeEquivalentTo(1))
	})

	ginkgo.It("preserves context cancellation", func() {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
		defer server.Close()
		client, err := NewPico(server.URL)
		Expect(err).NotTo(HaveOccurred())
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err = client.Head(ctx, "wait")
		Expect(err).To(MatchError(context.Canceled))
	})

	ginkgo.It("rejects H2C for HTTPS endpoints", func() {
		_, err := NewPico("https://example.com", WithH2C())
		Expect(err).To(MatchError("picomq: WithH2C requires an http endpoint"))
	})

	ginkgo.It("rejects HTTPS to HTTP ownership redirects", func() {
		var targetCalls atomic.Int32
		target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			targetCalls.Add(1)
		}))
		defer target.Close()
		origin := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, target.URL+r.URL.Path, http.StatusTemporaryRedirect)
		}))
		defer origin.Close()
		client, err := NewPico(origin.URL, WithHTTPClient(origin.Client()), WithToken("secret"))
		Expect(err).NotTo(HaveOccurred())
		_, err = client.Create(context.Background(), "orders", CreateOptions{ContentType: "application/octet-stream"})
		var clientErr *ClientError
		Expect(errors.As(err, &clientErr)).To(BeTrue())
		Expect(clientErr.Code).To(Equal("unsafe_redirect"))
		Expect(targetCalls.Load()).To(BeZero())
	})
})
