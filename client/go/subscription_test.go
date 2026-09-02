package picomq

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = ginkgo.Describe("SSE subscriptions", func() {
	ginkgo.It("decodes events and resumes with the last event ID", func() {
		var connections atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			if connections.Add(1) == 1 {
				Expect(r.URL.Query().Get("seq")).To(Equal("0"))
				_, _ = io.WriteString(w, "id: event-1\nevent: data\ndata: [{\"seq\":0,\"timestamp\":7,\"headers\":{},\"body\":\"hello\"}]\n\n")
				return
			}
			Expect(r.Header.Get("Last-Event-ID")).To(Equal("event-1"))
			Expect(r.URL.Query().Has("seq")).To(BeFalse())
			_, _ = io.WriteString(w, "event: control\ndata: {\"next_seq\":1,\"up_to_date\":true,\"closed\":true}\n\n")
		}))
		defer server.Close()

		client, err := NewPico(server.URL)
		Expect(err).NotTo(HaveOccurred())
		subscription := client.Subscribe(context.Background(), "events", "0", SubscribeOptions{ReconnectDelay: time.Millisecond})
		ginkgo.DeferCleanup(subscription.Close)

		event, err := subscription.Next()
		Expect(err).NotTo(HaveOccurred())
		Expect(event.Type).To(Equal(EventData))
		Expect(event.ID).To(Equal("event-1"))
		Expect(event.Records).To(ConsistOf(Record{Position: "0", Timestamp: 7, Headers: map[string]string{}, Body: []byte("hello")}))

		event, err = subscription.Next()
		Expect(err).NotTo(HaveOccurred())
		Expect(event.Type).To(Equal(EventControl))
		Expect(event.Next).To(Equal("1"))
		Expect(event.UpToDate).To(BeTrue())
		Expect(event.Closed).To(BeTrue())
		_, err = subscription.Next()
		Expect(err).To(MatchError(io.EOF))
	})

	ginkgo.It("decodes text and binary Pico keys", func() {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "event: data\ndata: [{\"seq\":0,\"key\":\"text\",\"headers\":{},\"body\":\"a\"},{\"seq\":1,\"key_b64\":\"AP8=\",\"headers\":{},\"body\":\"b\"}]\n\n")
		}))
		defer server.Close()
		client, err := NewPico(server.URL)
		Expect(err).NotTo(HaveOccurred())
		subscription := client.Subscribe(context.Background(), "events", "0", SubscribeOptions{DisableReconnect: true})
		ginkgo.DeferCleanup(subscription.Close)
		event, err := subscription.Next()
		Expect(err).NotTo(HaveOccurred())
		Expect(event.Records[0].Key).To(Equal([]byte("text")))
		Expect(event.Records[1].Key).To(Equal([]byte{0, 255}))
	})

	ginkgo.It("limits aggregate multiline event data", func() {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "data: 12345\ndata: 67890\n\n")
		}))
		defer server.Close()
		client, err := NewPico(server.URL)
		Expect(err).NotTo(HaveOccurred())
		subscription := client.Subscribe(context.Background(), "events", "0", SubscribeOptions{DisableReconnect: true, MaxEventBytes: 10})
		ginkgo.DeferCleanup(subscription.Close)
		_, err = subscription.Next()
		var clientErr *ClientError
		Expect(errors.As(err, &clientErr)).To(BeTrue())
		Expect(clientErr.Code).To(Equal("sse_event_too_large"))
	})

	ginkgo.It("can disable reconnects", func() {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Header().Set("Content-Type", "text/event-stream") }))
		defer server.Close()
		client, err := NewPico(server.URL)
		Expect(err).NotTo(HaveOccurred())
		subscription := client.Subscribe(context.Background(), "events", "0", SubscribeOptions{DisableReconnect: true})
		ginkgo.DeferCleanup(subscription.Close)
		_, err = subscription.Next()
		Expect(err).To(MatchError(io.EOF))
	})

	ginkgo.It("stops an active stream when closed", func() {
		started := make(chan struct{})
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(started); <-r.Context().Done() }))
		defer server.Close()
		client, err := NewPico(server.URL)
		Expect(err).NotTo(HaveOccurred())
		subscription := client.Subscribe(context.Background(), "events", "0", SubscribeOptions{})
		result := make(chan error, 1)
		go func() { _, nextErr := subscription.Next(); result <- nextErr }()
		Eventually(started).Should(BeClosed())
		Expect(subscription.Close()).To(Succeed())
		Eventually(result).Should(Receive(Or(Equal(context.Canceled), Equal(io.EOF))))
	})
})
