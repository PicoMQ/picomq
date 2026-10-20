package picomq

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = ginkgo.Describe("topic settings", func() {
	ginkgo.It("sends create options as each protocol's headers", func() {
		seen := make(chan http.Header, 2)
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			seen <- r.Header.Clone()
			w.WriteHeader(http.StatusCreated)
		}))
		defer server.Close()
		options := CreateOptions{Partitions: 3, Retention: time.Minute, RetentionBytes: 4096, Schema: "order.json", SchemaValidate: true}

		pico, err := NewPico(server.URL)
		Expect(err).NotTo(HaveOccurred())
		created, err := pico.Create(context.Background(), "/orders", options)
		Expect(err).NotTo(HaveOccurred())
		Expect(created).To(BeTrue())
		headers := <-seen
		Expect(headers.Get("Content-Type")).To(Equal("application/octet-stream"))
		Expect(headers.Get("Pico-Partitions")).To(Equal("3"))
		Expect(headers.Get("Pico-Retention-Ms")).To(Equal("60000"))
		Expect(headers.Get("Pico-Retention-Bytes")).To(Equal("4096"))
		Expect(headers.Get("Pico-Schema")).To(Equal("order.json"))
		Expect(headers.Get("Pico-Schema-Validate")).To(Equal("true"))
		Expect(headers.Get("Pico-TTL")).To(BeEmpty())

		ds, err := NewDurableStreams(server.URL)
		Expect(err).NotTo(HaveOccurred())
		_, err = ds.Create(context.Background(), "/orders", options)
		Expect(err).NotTo(HaveOccurred())
		headers = <-seen
		Expect(headers.Get("Stream-Partitions")).To(Equal("3"))
		Expect(headers.Get("Stream-Retention-Ms")).To(Equal("60000"))
	})

	ginkgo.It("patches only the fields it sets and removes zeroed ones", func() {
		var body map[string]any
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			Expect(r.Method).To(Equal(http.MethodPatch))
			Expect(r.URL.Path).To(Equal("/_streams/orders"))
			data, _ := io.ReadAll(r.Body)
			Expect(json.Unmarshal(data, &body)).To(Succeed())
			_, _ = w.Write([]byte(`{"schema":null,"schemaValidate":false,"kafkaTopic":"orders","partitions":4,"retentionMs":null,"retentionBytes":4096}`))
		}))
		defer server.Close()
		client, err := NewPico(server.URL)
		Expect(err).NotTo(HaveOccurred())

		none, limit := time.Duration(0), uint64(4096)
		config, err := client.UpdateConfig(context.Background(), "/orders", ConfigUpdate{Retention: &none, RetentionBytes: &limit, Partitions: 4})
		Expect(err).NotTo(HaveOccurred())
		Expect(body).To(Equal(map[string]any{"retentionMs": nil, "retentionBytes": float64(4096), "partitions": float64(4)}))
		Expect(config).To(Equal(TopicConfig{KafkaTopic: "orders", Partitions: 4, RetentionBytes: 4096}))
	})

	ginkgo.It("never rounds a nonzero retention to unlimited", func() {
		var bodies []map[string]any
		var headers []http.Header
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPut {
				headers = append(headers, r.Header.Clone())
				w.WriteHeader(http.StatusCreated)
				return
			}
			var body map[string]any
			data, _ := io.ReadAll(r.Body)
			Expect(json.Unmarshal(data, &body)).To(Succeed())
			bodies = append(bodies, body)
			_, _ = w.Write([]byte(`{"partitions":1}`))
		}))
		defer server.Close()
		client, err := NewPico(server.URL)
		Expect(err).NotTo(HaveOccurred())

		_, err = client.Create(context.Background(), "/orders", CreateOptions{Retention: 500 * time.Microsecond})
		Expect(err).NotTo(HaveOccurred())
		for _, retention := range []time.Duration{500 * time.Microsecond, -500 * time.Microsecond, 0} {
			_, err = client.UpdateConfig(context.Background(), "/orders", ConfigUpdate{Retention: &retention})
			Expect(err).NotTo(HaveOccurred())
		}

		Expect(headers[0].Get("Pico-Retention-Ms")).To(Equal("1"))
		Expect(bodies[0]["retentionMs"]).To(Equal(float64(1)))
		Expect(bodies[1]["retentionMs"]).To(Equal(float64(-1)))
		Expect(bodies[2]).To(HaveKeyWithValue("retentionMs", BeNil()))
	})

	ginkgo.It("escapes schema names in the path", func() {
		paths := make(chan string, 1)
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			paths <- r.URL.EscapedPath()
			w.WriteHeader(http.StatusNoContent)
		}))
		defer server.Close()
		client, err := NewPico(server.URL)
		Expect(err).NotTo(HaveOccurred())

		_, err = client.DeleteSchema(context.Background(), "team/orders#v2 final.json")
		Expect(err).NotTo(HaveOccurred())
		Expect(<-paths).To(Equal("/_schemas/team/orders%23v2%20final.json"))
	})

	ginkgo.It("decodes control route errors and missing topics and schemas", func() {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.Method {
			case http.MethodPatch:
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"error":"partitions must grow"}`))
			default:
				w.WriteHeader(http.StatusNotFound)
			}
		}))
		defer server.Close()
		client, err := NewDurableStreams(server.URL)
		Expect(err).NotTo(HaveOccurred())

		_, err = client.UpdateConfig(context.Background(), "/orders", ConfigUpdate{Partitions: 1})
		Expect(IsKind(err, ErrorBadRequest)).To(BeTrue())
		Expect(err.(*ClientError).Message).To(Equal("partitions must grow"))
		config, err := client.Config(context.Background(), "/orders")
		Expect(err).NotTo(HaveOccurred())
		Expect(config).To(BeNil())
		schema, err := client.GetSchema(context.Background(), "order.json")
		Expect(err).NotTo(HaveOccurred())
		Expect(schema).To(BeNil())
		deleted, err := client.DeleteSchema(context.Background(), "order.json")
		Expect(err).NotTo(HaveOccurred())
		Expect(deleted).To(BeFalse())
	})
})
