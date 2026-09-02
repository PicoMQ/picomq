package picomq

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"

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

	ginkgo.It("reports unsupported union operations as structured errors", func() {
		client, err := NewDurableStreams("http://127.0.0.1:4437")
		Expect(err).NotTo(HaveOccurred())
		_, err = client.List(context.Background(), "", 0)
		Expect(IsKind(err, ErrorUnsupported)).To(BeTrue())
		var clientError *ClientError
		Expect(errors.As(err, &clientError)).To(BeTrue())
		Expect(clientError.Code).To(Equal("unsupported"))
	})
})
