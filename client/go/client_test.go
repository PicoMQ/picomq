package picomq

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"net/http"
	"net/http/httptest"
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
		created, err := client.Create(context.Background(), "/orders", "application/json", 0)
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
		_, err = client.Create(context.Background(), "orders", "application/octet-stream", time.Second+time.Nanosecond)
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
		created, err := client.Create(context.Background(), "redirected", "application/octet-stream", 0)
		Expect(err).NotTo(HaveOccurred())
		Expect(created).To(BeTrue())
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
		_, err = client.Create(context.Background(), "orders", "application/octet-stream", 0)
		var clientErr *ClientError
		Expect(errors.As(err, &clientErr)).To(BeTrue())
		Expect(clientErr.Code).To(Equal("unsafe_redirect"))
		Expect(targetCalls.Load()).To(BeZero())
	})
})
