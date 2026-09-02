package picomq

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = ginkgo.Describe("live PicoMQ", ginkgo.Label("integration"), func() {
	ginkgo.It("round trips through a running Pico server", func() {
		if os.Getenv("PICOMQ_INTEGRATION") == "" {
			ginkgo.Skip("set PICOMQ_INTEGRATION=1 to run live tests")
		}
		endpoint := os.Getenv("PICOMQ_ENDPOINT")
		if endpoint == "" {
			endpoint = "http://127.0.0.1:4437"
		}
		client, err := NewPico(endpoint)
		Expect(err).NotTo(HaveOccurred())
		name := fmt.Sprintf("/go-client-tests/%d", time.Now().UnixNano())
		stream := client.Stream(name)
		ginkgo.DeferCleanup(func() { _, _ = stream.Delete(context.Background()) })
		created, err := stream.Create(context.Background(), "application/octet-stream", 0)
		Expect(err).NotTo(HaveOccurred())
		Expect(created).To(BeTrue())
		ack, err := stream.Append(context.Background(), AppendRecord{Body: []byte("hello"), Headers: map[string]string{"source": "go"}})
		Expect(err).NotTo(HaveOccurred())
		page, err := stream.Read(context.Background(), client.Beginning(), ReadOptions{})
		Expect(err).NotTo(HaveOccurred())
		Expect(page.Next).To(Equal(ack.Next))
		Expect(page.Records).To(HaveLen(1))
		Expect(page.Records[0].Body).To(Equal([]byte("hello")))

		producerConfig := DefaultProducerConfig()
		producerConfig.Linger = 0
		producer := stream.NewProducer("go-integration", &producerConfig)
		seq, err := producer.SendDurable(context.Background(), AppendRecord{Body: []byte("produced")})
		Expect(err).NotTo(HaveOccurred())
		Expect(seq).To(Equal(uint64(1)))
		Expect(producer.Close(context.Background())).To(Succeed())

		subscriptionContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		subscription := stream.Subscribe(subscriptionContext, client.Beginning(), SubscribeOptions{DisableReconnect: true})
		ginkgo.DeferCleanup(subscription.Close)
		var bodies [][]byte
		for len(bodies) < 2 {
			event, nextErr := subscription.Next()
			Expect(nextErr).NotTo(HaveOccurred())
			for _, record := range event.Records {
				bodies = append(bodies, record.Body)
			}
		}
		Expect(bodies).To(ConsistOf([]byte("hello"), []byte("produced")))
	})

	ginkgo.It("round trips through a running Durable Streams server", func() {
		if os.Getenv("PICOMQ_DS_INTEGRATION") == "" {
			ginkgo.Skip("set PICOMQ_DS_INTEGRATION=1 to run live Durable Streams tests")
		}
		endpoint := os.Getenv("PICOMQ_DS_ENDPOINT")
		if endpoint == "" {
			endpoint = "http://127.0.0.1:4437"
		}
		client, err := NewDurableStreams(endpoint)
		Expect(err).NotTo(HaveOccurred())
		name := fmt.Sprintf("/go-client-tests/ds-%d", time.Now().UnixNano())
		stream := client.Stream(name)
		ginkgo.DeferCleanup(func() { _, _ = stream.Delete(context.Background()) })

		created, err := stream.Create(context.Background(), "application/octet-stream", 0)
		Expect(err).NotTo(HaveOccurred())
		Expect(created).To(BeTrue())
		ack, err := stream.Append(context.Background(), AppendRecord{Body: []byte("hello-ds")})
		Expect(err).NotTo(HaveOccurred())
		Expect(ack.Next).NotTo(BeEmpty())

		page, err := stream.Read(context.Background(), client.Beginning(), ReadOptions{})
		Expect(err).NotTo(HaveOccurred())
		Expect(page.Records).To(HaveLen(1))
		Expect(page.Records[0].Body).To(Equal([]byte("hello-ds")))

		subscriptionContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		subscription := stream.Subscribe(subscriptionContext, client.Beginning(), SubscribeOptions{DisableReconnect: true})
		ginkgo.DeferCleanup(subscription.Close)
		for {
			event, nextErr := subscription.Next()
			Expect(nextErr).NotTo(HaveOccurred())
			if len(event.Records) > 0 {
				Expect(event.Records[0].Body).To(Equal([]byte("hello-ds")))
				break
			}
		}
	})
})
