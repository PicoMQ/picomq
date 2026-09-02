package picomq

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func liveEndpoint(variable string) string {
	if endpoint := os.Getenv(variable); endpoint != "" {
		return endpoint
	}
	return "http://127.0.0.1:4437"
}

func liveOptions() []Option {
	options := []Option{WithRetryPolicy(RetryAttempts(3))}
	if token := os.Getenv("PICOMQ_TOKEN"); token != "" {
		options = append(options, WithToken(token))
	}
	return options
}

// rawProbe issues one GET without following redirects so a test can observe
// which cluster node owns a stream and what the other node answers.
func rawProbe(endpoint, name, query string) (int, *url.URL) {
	request, err := http.NewRequest(http.MethodGet, endpoint+name+"?"+query, nil)
	Expect(err).NotTo(HaveOccurred())
	if token := os.Getenv("PICOMQ_TOKEN"); token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	probe := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := probe.Do(request)
	Expect(err).NotTo(HaveOccurred())
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, response.Body)
	var location *url.URL
	if raw := response.Header.Get("Location"); raw != "" {
		location, err = url.Parse(raw)
		Expect(err).NotTo(HaveOccurred())
	}
	return response.StatusCode, location
}

// splitOwnership returns (owner, follower) client indexes for a stream that
// has been opened on a two-node cluster, asserting that exactly one node
// serves it and the other 307s to that node with the query preserved.
func splitOwnership(endpoints [2]string, name, query string) (owner, follower int) {
	statuses := [2]int{}
	locations := [2]*url.URL{}
	for i, endpoint := range endpoints {
		statuses[i], locations[i] = rawProbe(endpoint, name, query)
	}
	Expect(statuses[:]).To(ConsistOf(http.StatusOK, http.StatusTemporaryRedirect), "one node must own the stream and the other must redirect")
	if statuses[0] == http.StatusTemporaryRedirect {
		owner, follower = 1, 0
	} else {
		owner, follower = 0, 1
	}
	ownerURL, err := url.Parse(endpoints[owner])
	Expect(err).NotTo(HaveOccurred())
	location := locations[follower]
	Expect(location).NotTo(BeNil())
	Expect(location.Port()).To(Equal(ownerURL.Port()), "redirect must point at the owning node")
	Expect(location.Path).To(Equal(name))
	Expect(location.RawQuery).To(Equal(query), "redirect must preserve the query")
	return owner, follower
}

// collectBodies drains a subscription until want bodies have arrived (or
// the stream reports closed), returning them in arrival order.
func collectBodies(subscription *Subscription, want int) []string {
	var bodies []string
	for len(bodies) < want {
		event, err := subscription.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		Expect(err).NotTo(HaveOccurred())
		for _, record := range event.Records {
			bodies = append(bodies, string(record.Body))
		}
	}
	return bodies
}

var _ = ginkgo.Describe("live PicoMQ", ginkgo.Label("integration"), func() {
	ginkgo.It("round trips through a running Pico server", func() {
		if os.Getenv("PICOMQ_INTEGRATION") == "" {
			ginkgo.Skip("set PICOMQ_INTEGRATION=1 to run live tests")
		}
		client, err := NewPico(liveEndpoint("PICOMQ_ENDPOINT"), liveOptions()...)
		Expect(err).NotTo(HaveOccurred())
		name := fmt.Sprintf("/go-client-tests/%d", time.Now().UnixNano())
		stream := client.Stream(name)
		ginkgo.DeferCleanup(func() { _, _ = stream.Delete(context.Background()) })
		created, err := stream.Create(context.Background(), "application/octet-stream", 0)
		Expect(err).NotTo(HaveOccurred())
		Expect(created).To(BeTrue())
		ack, err := stream.Append(context.Background(), AppendRecord{Body: []byte("hello"), Headers: map[string][]byte{"source": []byte("go")}})
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

	ginkgo.It("covers paging, now, trim, close, and producer resume on a running Pico server", func() {
		if os.Getenv("PICOMQ_INTEGRATION") == "" {
			ginkgo.Skip("set PICOMQ_INTEGRATION=1 to run live tests")
		}
		ctx := context.Background()
		client, err := NewPico(liveEndpoint("PICOMQ_ENDPOINT"), liveOptions()...)
		Expect(err).NotTo(HaveOccurred())
		name := fmt.Sprintf("/go-client-tests/features-%d", time.Now().UnixNano())
		stream := client.Stream(name)
		ginkgo.DeferCleanup(func() { _, _ = stream.Delete(ctx) })
		_, err = stream.Create(ctx, "application/octet-stream", 0)
		Expect(err).NotTo(HaveOccurred())

		records := make([]AppendRecord, 10)
		for i := range records {
			records[i] = AppendRecord{Body: []byte(fmt.Sprintf("record-%02d", i)), Key: []byte(fmt.Sprint(i % 3))}
		}
		ack, err := stream.Append(ctx, records...)
		Expect(err).NotTo(HaveOccurred())
		Expect(ack.Start).To(Equal("0"))
		Expect(ack.Next).To(Equal("10"))

		page, err := stream.Read(ctx, "0", ReadOptions{Limits: ReadLimits{Bytes: 25}})
		Expect(err).NotTo(HaveOccurred())
		Expect(page.Records).ToNot(BeEmpty())
		Expect(len(page.Records)).To(BeNumerically("<", 10))
		Expect(page.UpToDate).To(BeFalse())

		iterator := stream.Records(RecordsOptions{Limits: ReadLimits{Count: 3}})
		var bodies []string
		for {
			record, nextErr := iterator.Next(ctx)
			if errors.Is(nextErr, io.EOF) {
				break
			}
			Expect(nextErr).NotTo(HaveOccurred())
			Expect(record.Key).To(Equal([]byte(fmt.Sprint(len(bodies) % 3))))
			bodies = append(bodies, string(record.Body))
		}
		Expect(bodies).To(HaveLen(10))
		Expect(bodies[9]).To(Equal("record-09"))
		Expect(iterator.Position()).To(Equal("10"))

		page, err = stream.Read(ctx, "now", ReadOptions{})
		Expect(err).NotTo(HaveOccurred())
		Expect(page.Records).To(BeEmpty())
		Expect(page.Next).To(Equal("10"))
		Expect(page.UpToDate).To(BeTrue())

		producer := ProducerRef{ID: "resume", Epoch: 1, Seq: 0}
		first, err := stream.AppendAs(ctx, []AppendRecord{{Body: []byte("p0")}}, producer)
		Expect(err).NotTo(HaveOccurred())
		Expect(first.Applied).To(BeTrue())
		Expect(first.Ack.Start).To(Equal("10"))
		config := DefaultProducerConfig()
		config.Epoch = 1
		config.Linger = 0
		resumed := stream.NewProducer("resume", &config)
		seq, err := resumed.SendDurable(ctx, AppendRecord{Body: []byte("p0")})
		Expect(err).NotTo(HaveOccurred())
		Expect(seq).To(Equal(uint64(10)))
		seq, err = resumed.SendDurable(ctx, AppendRecord{Body: []byte("p1")})
		Expect(err).NotTo(HaveOccurred())
		Expect(seq).To(Equal(uint64(11)))
		Expect(resumed.Close(ctx)).To(Succeed())
		info, err := stream.Head(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(info.Next).To(Equal("12"))

		var start string
		for attempt := 0; attempt < 100 && start != "5"; attempt++ {
			start, err = stream.Trim(ctx, 5)
			Expect(err).NotTo(HaveOccurred())
			time.Sleep(50 * time.Millisecond)
		}
		Expect(start).To(Equal("5"))
		info, err = stream.Head(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(info.Start).To(Equal("5"))

		next, err := stream.Close(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(next).To(Equal("12"))
		_, err = stream.Append(ctx, AppendRecord{Body: []byte("late")})
		Expect(IsKind(err, ErrorClosed)).To(BeTrue())
		subscriptionContext, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		subscription := stream.Subscribe(subscriptionContext, "5", SubscribeOptions{DisableReconnect: true})
		closed := false
		for {
			event, nextErr := subscription.Next()
			if errors.Is(nextErr, io.EOF) {
				break
			}
			Expect(nextErr).NotTo(HaveOccurred())
			closed = closed || event.Closed
		}
		Expect(closed).To(BeTrue())
		deleted, err := stream.Delete(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(deleted).To(BeTrue())
		info, err = stream.Head(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(info).To(BeNil())
	})

	ginkgo.It("bounds SSE event sizes on a running Pico server", func() {
		if os.Getenv("PICOMQ_INTEGRATION") == "" {
			ginkgo.Skip("set PICOMQ_INTEGRATION=1 to run live tests")
		}
		ctx := context.Background()
		client, err := NewPico(liveEndpoint("PICOMQ_ENDPOINT"), liveOptions()...)
		Expect(err).NotTo(HaveOccurred())
		name := fmt.Sprintf("/go-client-tests/sse-%d", time.Now().UnixNano())
		stream := client.Stream(name)
		ginkgo.DeferCleanup(func() { _, _ = stream.Delete(ctx) })
		_, err = stream.Create(ctx, "application/octet-stream", 0)
		Expect(err).NotTo(HaveOccurred())
		large := bytes.Repeat([]byte{0xAB}, 256<<10)
		_, err = stream.Append(ctx, AppendRecord{Body: large})
		Expect(err).NotTo(HaveOccurred())

		subscriptionContext, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		small := stream.Subscribe(subscriptionContext, "0", SubscribeOptions{DisableReconnect: true, MaxEventBytes: 1024})
		ginkgo.DeferCleanup(small.Close)
		var clientErr *ClientError
		for {
			_, nextErr := small.Next()
			if nextErr != nil {
				Expect(errors.As(nextErr, &clientErr)).To(BeTrue())
				Expect(clientErr.Code).To(Equal("sse_event_too_large"))
				break
			}
		}

		full := stream.Subscribe(subscriptionContext, "0", SubscribeOptions{DisableReconnect: true})
		ginkgo.DeferCleanup(full.Close)
		for {
			event, nextErr := full.Next()
			Expect(nextErr).NotTo(HaveOccurred())
			if len(event.Records) > 0 {
				Expect(event.Records[0].Body).To(Equal(large))
				break
			}
		}
	})

	ginkgo.It("enforces bearer tokens on a running Pico server", func() {
		if os.Getenv("PICOMQ_INTEGRATION") == "" || os.Getenv("PICOMQ_TOKEN") == "" {
			ginkgo.Skip("set PICOMQ_INTEGRATION=1 and PICOMQ_TOKEN to run auth tests")
		}
		ctx := context.Background()
		endpoint := liveEndpoint("PICOMQ_ENDPOINT")
		name := fmt.Sprintf("/go-client-tests/auth-%d", time.Now().UnixNano())
		anonymous, err := NewPico(endpoint)
		Expect(err).NotTo(HaveOccurred())
		_, err = anonymous.Create(ctx, name, "application/octet-stream", 0)
		Expect(IsKind(err, ErrorUnauthenticated)).To(BeTrue())
		_, err = anonymous.Read(ctx, name, "0", ReadOptions{})
		Expect(IsKind(err, ErrorUnauthenticated)).To(BeTrue())
		subscription := anonymous.Subscribe(ctx, name, "0", SubscribeOptions{DisableReconnect: true})
		_, err = subscription.Next()
		Expect(IsKind(err, ErrorUnauthenticated)).To(BeTrue())
		_ = subscription.Close()
		wrong, err := NewPico(endpoint, WithToken("not-a-token"))
		Expect(err).NotTo(HaveOccurred())
		_, err = wrong.Create(ctx, name, "application/octet-stream", 0)
		Expect(IsKind(err, ErrorUnauthenticated)).To(BeTrue())

		client, err := NewPico(endpoint, WithToken(os.Getenv("PICOMQ_TOKEN")))
		Expect(err).NotTo(HaveOccurred())
		stream := client.Stream(name)
		ginkgo.DeferCleanup(func() { _, _ = stream.Delete(ctx) })
		created, err := stream.Create(ctx, "application/octet-stream", 0)
		Expect(err).NotTo(HaveOccurred())
		Expect(created).To(BeTrue())
		_, err = stream.Append(ctx, AppendRecord{Body: []byte("secret")})
		Expect(err).NotTo(HaveOccurred())
		page, err := stream.Read(ctx, "0", ReadOptions{})
		Expect(err).NotTo(HaveOccurred())
		Expect(page.Records).To(HaveLen(1))
		subscriptionContext, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		authorized := stream.Subscribe(subscriptionContext, "0", SubscribeOptions{DisableReconnect: true})
		ginkgo.DeferCleanup(authorized.Close)
		for {
			event, nextErr := authorized.Next()
			Expect(nextErr).NotTo(HaveOccurred())
			if len(event.Records) > 0 {
				Expect(event.Records[0].Body).To(Equal([]byte("secret")))
				break
			}
		}
	})

	ginkgo.It("follows ownership redirects across a running two-node Pico cluster", func() {
		if os.Getenv("PICOMQ_INTEGRATION") == "" || os.Getenv("PICOMQ_ENDPOINT_2") == "" {
			ginkgo.Skip("set PICOMQ_INTEGRATION=1 and PICOMQ_ENDPOINT_2 to run cluster tests")
		}
		ctx := context.Background()
		endpoints := [2]string{liveEndpoint("PICOMQ_ENDPOINT"), os.Getenv("PICOMQ_ENDPOINT_2")}
		clients := [2]*PicoClient{}
		for i, endpoint := range endpoints {
			client, err := NewPico(endpoint, liveOptions()...)
			Expect(err).NotTo(HaveOccurred())
			clients[i] = client
		}
		name := fmt.Sprintf("/go-client-tests/cluster-%d", time.Now().UnixNano())
		ginkgo.DeferCleanup(func() { _, _ = clients[0].Delete(ctx, name) })
		created, err := clients[0].Create(ctx, name, "application/octet-stream", 0)
		Expect(err).NotTo(HaveOccurred())
		Expect(created).To(BeTrue())
		seed := []AppendRecord{{Body: []byte("r0")}, {Body: []byte("r1")}, {Body: []byte("r2")}}
		ack, err := clients[0].Append(ctx, name, seed)
		Expect(err).NotTo(HaveOccurred())
		Expect(ack.Next).To(Equal("3"))

		ownerIndex, followerIndex := splitOwnership(endpoints, name, "seq=0&limit=1")
		owner, follower := clients[ownerIndex].Stream(name), clients[followerIndex].Stream(name)

		// Every non-PUT operation through the follower is a real 307 hop.
		info, err := follower.Head(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(info.Next).To(Equal("3"))
		page, err := follower.Read(ctx, "1", ReadOptions{Limits: ReadLimits{Count: 1}})
		Expect(err).NotTo(HaveOccurred())
		Expect(page.Records).To(HaveLen(1))
		Expect(page.Records[0].Body).To(Equal([]byte("r1")))
		Expect(page.Next).To(Equal("2"))
		ack, err = follower.Append(ctx, AppendRecord{Body: []byte("r3"), Key: []byte("k"), Headers: map[string][]byte{"via": []byte("follower")}})
		Expect(err).NotTo(HaveOccurred())
		Expect(ack.Start).To(Equal("3"))
		producerAck, err := follower.AppendAs(ctx, []AppendRecord{{Body: []byte("r4")}}, ProducerRef{ID: "cluster", Epoch: 1, Seq: 0})
		Expect(err).NotTo(HaveOccurred())
		Expect(producerAck.Applied).To(BeTrue())
		Expect(producerAck.Ack.Start).To(Equal("4"))
		producerAck, err = follower.AppendAs(ctx, []AppendRecord{{Body: []byte("r4")}}, ProducerRef{ID: "cluster", Epoch: 1, Seq: 0})
		Expect(err).NotTo(HaveOccurred())
		Expect(producerAck.Duplicate).To(BeTrue())
		config := DefaultProducerConfig()
		config.Epoch = 2
		config.Linger = 0
		producer := follower.NewProducer("cluster", &config)
		seq, err := producer.SendDurable(ctx, AppendRecord{Body: []byte("r5")})
		Expect(err).NotTo(HaveOccurred())
		Expect(seq).To(Equal(uint64(5)))
		Expect(producer.Close(ctx)).To(Succeed())
		_, err = follower.AppendAs(ctx, []AppendRecord{{Body: []byte("stale")}}, ProducerRef{ID: "cluster", Epoch: 1, Seq: 1})
		Expect(IsKind(err, ErrorStaleEpoch)).To(BeTrue())

		expected := []string{"r0", "r1", "r2", "r3", "r4", "r5"}
		subscriptionContext, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		subscription := follower.Subscribe(subscriptionContext, "0", SubscribeOptions{DisableReconnect: true})
		ginkgo.DeferCleanup(subscription.Close)
		Expect(collectBodies(subscription, len(expected))).To(Equal(expected))
		iterator := follower.Records(RecordsOptions{Limits: ReadLimits{Count: 2}})
		var iterated []string
		for {
			record, nextErr := iterator.Next(ctx)
			if errors.Is(nextErr, io.EOF) {
				break
			}
			Expect(nextErr).NotTo(HaveOccurred())
			iterated = append(iterated, string(record.Body))
		}
		Expect(iterated).To(Equal(expected))
		record4, err := owner.Read(ctx, "3", ReadOptions{Limits: ReadLimits{Count: 1}})
		Expect(err).NotTo(HaveOccurred())
		Expect(record4.Records[0].Key).To(Equal([]byte("k")))
		Expect(record4.Records[0].Headers["via"]).To(Equal([]byte("follower")))

		for _, client := range clients {
			listing, err := client.List(ctx, "/go-client-tests/cluster-", 100)
			Expect(err).NotTo(HaveOccurred())
			var names []string
			for _, entry := range listing.Streams {
				names = append(names, entry.Name)
			}
			Expect(names).To(ContainElement(name))
		}

		var start string
		for attempt := 0; attempt < 100 && start != "2"; attempt++ {
			start, err = follower.Trim(ctx, 2)
			Expect(err).NotTo(HaveOccurred())
			time.Sleep(50 * time.Millisecond)
		}
		Expect(start).To(Equal("2"))
		next, err := follower.Close(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(next).To(Equal("6"))
		for _, stream := range []*PicoStream{owner, follower} {
			info, err = stream.Head(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(info.Closed).To(BeTrue())
			Expect(info.Start).To(Equal("2"))
			Expect(info.Next).To(Equal("6"))
		}
		deleted, err := follower.Delete(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(deleted).To(BeTrue())
		info, err = owner.Head(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(info).To(BeNil())
	})

	ginkgo.It("resumes subscriptions and producers across a Pico server restart", func() {
		if os.Getenv("PICOMQ_INTEGRATION") == "" || os.Getenv("PICOMQ_RESTART_CMD") == "" {
			ginkgo.Skip("set PICOMQ_INTEGRATION=1 and PICOMQ_RESTART_CMD to run restart tests")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		options := append(liveOptions(), WithRetryPolicy(RetryAttempts(6)))
		client, err := NewPico(liveEndpoint("PICOMQ_ENDPOINT"), options...)
		Expect(err).NotTo(HaveOccurred())
		name := fmt.Sprintf("/go-client-tests/restart-%d", time.Now().UnixNano())
		stream := client.Stream(name)
		ginkgo.DeferCleanup(func() { _, _ = stream.Delete(context.Background()) })
		_, err = stream.Create(ctx, "application/octet-stream", 0)
		Expect(err).NotTo(HaveOccurred())
		const seeded, produced = 3, 80
		seed := make([]AppendRecord, seeded)
		expected := make([]string, 0, seeded+produced)
		for i := range seed {
			seed[i] = AppendRecord{Body: []byte(fmt.Sprintf("seed-%d", i))}
			expected = append(expected, string(seed[i].Body))
		}
		_, err = stream.Append(ctx, seed...)
		Expect(err).NotTo(HaveOccurred())

		subscription := stream.Subscribe(ctx, "0", SubscribeOptions{ReconnectDelay: 250 * time.Millisecond, MaxReconnectDelay: 2 * time.Second})
		ginkgo.DeferCleanup(subscription.Close)
		Expect(collectBodies(subscription, seeded)).To(Equal(expected))

		config := DefaultProducerConfig()
		config.Linger = 0
		config.Retry = RetryAttempts(12)
		producer := stream.NewProducer("restart", &config)
		ginkgo.DeferCleanup(func() { _ = producer.Close(context.Background()) })
		type sent struct {
			seq uint64
			err error
		}
		results := make(chan sent, produced)
		go func() {
			for i := 0; i < produced; i++ {
				seq, sendErr := producer.SendDurable(ctx, AppendRecord{Body: []byte(fmt.Sprintf("live-%02d", i))})
				results <- sent{seq: seq, err: sendErr}
				time.Sleep(100 * time.Millisecond)
			}
		}()
		for i := 0; i < produced; i++ {
			expected = append(expected, fmt.Sprintf("live-%02d", i))
		}

		var seqs []uint64
		for len(seqs) < 5 {
			result := <-results
			Expect(result.err).NotTo(HaveOccurred())
			seqs = append(seqs, result.seq)
		}
		restart := exec.CommandContext(ctx, "sh", "-c", os.Getenv("PICOMQ_RESTART_CMD"))
		output, err := restart.CombinedOutput()
		Expect(err).NotTo(HaveOccurred(), string(output))
		for len(seqs) < produced {
			result := <-results
			Expect(result.err).NotTo(HaveOccurred(), "producer must survive the restart without poisoning")
			seqs = append(seqs, result.seq)
		}
		for i, seq := range seqs {
			Expect(seq).To(Equal(uint64(seeded+i)), "producer positions must stay gap-free and duplicate-free across the restart")
		}

		Expect(collectBodies(subscription, produced)).To(Equal(expected[seeded:]), "subscription must resume exactly once from where it dropped")
		info, err := stream.Head(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(info.Next).To(Equal(fmt.Sprint(seeded + produced)))
		page, err := stream.Read(ctx, "0", ReadOptions{Limits: ReadLimits{Count: 1000}})
		Expect(err).NotTo(HaveOccurred())
		var stored []string
		for _, record := range page.Records {
			stored = append(stored, string(record.Body))
		}
		Expect(stored).To(Equal(expected))
	})

	ginkgo.It("follows ownership redirects across a running two-node Durable Streams cluster", func() {
		if os.Getenv("PICOMQ_DS_INTEGRATION") == "" || os.Getenv("PICOMQ_DS_ENDPOINT_2") == "" {
			ginkgo.Skip("set PICOMQ_DS_INTEGRATION=1 and PICOMQ_DS_ENDPOINT_2 to run cluster tests")
		}
		ctx := context.Background()
		endpoints := [2]string{liveEndpoint("PICOMQ_DS_ENDPOINT"), os.Getenv("PICOMQ_DS_ENDPOINT_2")}
		clients := [2]*DurableStreamsClient{}
		for i, endpoint := range endpoints {
			client, err := NewDurableStreams(endpoint, liveOptions()...)
			Expect(err).NotTo(HaveOccurred())
			clients[i] = client
		}
		name := fmt.Sprintf("/go-client-tests/ds-cluster-%d", time.Now().UnixNano())
		ginkgo.DeferCleanup(func() { _, _ = clients[0].Delete(ctx, name) })
		created, err := clients[0].Create(ctx, name, "text/plain", 0)
		Expect(err).NotTo(HaveOccurred())
		Expect(created).To(BeTrue())
		_, err = clients[0].Append(ctx, name, []AppendRecord{{Body: []byte("first"), ContentType: "text/plain"}})
		Expect(err).NotTo(HaveOccurred())

		ownerIndex, followerIndex := splitOwnership(endpoints, name, "offset=-1")
		owner, follower := clients[ownerIndex].Stream(name), clients[followerIndex].Stream(name)
		info, err := follower.Head(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(info.ContentType).To(HavePrefix("text/plain"))
		_, err = follower.Append(ctx, AppendRecord{Body: []byte("second"), ContentType: "text/plain"})
		Expect(err).NotTo(HaveOccurred())
		page, err := follower.Read(ctx, clients[0].Beginning(), ReadOptions{})
		Expect(err).NotTo(HaveOccurred())
		Expect(page.Records).ToNot(BeEmpty())
		var joined []byte
		for _, record := range page.Records {
			joined = append(joined, record.Body...)
		}
		Expect(string(joined)).To(Equal("firstsecond"))
		Expect(page.UpToDate).To(BeTrue())

		subscriptionContext, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		subscription := follower.Subscribe(subscriptionContext, clients[0].Beginning(), SubscribeOptions{DisableReconnect: true})
		ginkgo.DeferCleanup(subscription.Close)
		var streamed []byte
		for len(streamed) < len("firstsecond") {
			event, nextErr := subscription.Next()
			Expect(nextErr).NotTo(HaveOccurred())
			for _, record := range event.Records {
				streamed = append(streamed, record.Body...)
			}
		}
		Expect(string(streamed)).To(Equal("firstsecond"))

		next, err := follower.Close(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(next).To(Equal(page.Next))
		for _, stream := range []*Stream{owner, follower} {
			info, err = stream.Head(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(info.Closed).To(BeTrue())
		}
		deleted, err := follower.Delete(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(deleted).To(BeTrue())
	})

	ginkgo.It("covers TTL, now, and closing on a running Durable Streams server", func() {
		if os.Getenv("PICOMQ_DS_INTEGRATION") == "" {
			ginkgo.Skip("set PICOMQ_DS_INTEGRATION=1 to run live Durable Streams tests")
		}
		ctx := context.Background()
		client, err := NewDurableStreams(liveEndpoint("PICOMQ_DS_ENDPOINT"), liveOptions()...)
		Expect(err).NotTo(HaveOccurred())
		name := fmt.Sprintf("/go-client-tests/ds-features-%d", time.Now().UnixNano())
		stream := client.Stream(name)
		ginkgo.DeferCleanup(func() { _, _ = stream.Delete(ctx) })
		created, err := stream.Create(ctx, "text/plain", 90*time.Second)
		Expect(err).NotTo(HaveOccurred())
		Expect(created).To(BeTrue())
		info, err := stream.Head(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(info.TTL).To(Equal(90 * time.Second))
		Expect(info.ContentType).To(HavePrefix("text/plain"))

		_, err = stream.Append(ctx, AppendRecord{Body: []byte("first"), ContentType: "text/plain"})
		Expect(err).NotTo(HaveOccurred())
		_, err = stream.Append(ctx, AppendRecord{Body: []byte("mismatch")})
		Expect(IsKind(err, ErrorConflict)).To(BeTrue())
		now, err := client.Now()
		Expect(err).NotTo(HaveOccurred())
		page, err := stream.Read(ctx, now, ReadOptions{})
		Expect(err).NotTo(HaveOccurred())
		Expect(page.Records).To(BeEmpty())
		Expect(page.UpToDate).To(BeTrue())
		tail := page.Next

		go func() {
			time.Sleep(300 * time.Millisecond)
			_, _ = stream.Append(ctx, AppendRecord{Body: []byte("second"), ContentType: "text/plain"})
		}()
		pollContext, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		page, err = stream.Read(pollContext, tail, ReadOptions{Live: LiveLongPoll})
		Expect(err).NotTo(HaveOccurred())
		Expect(page.Records).To(HaveLen(1))
		Expect(page.Records[0].Body).To(Equal([]byte("second")))

		iterator := stream.Records(RecordsOptions{})
		var chunks int
		for {
			_, nextErr := iterator.Next(ctx)
			if errors.Is(nextErr, io.EOF) {
				break
			}
			Expect(nextErr).NotTo(HaveOccurred())
			chunks++
		}
		Expect(chunks).To(BeNumerically(">=", 1))

		next, err := stream.Close(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(next).NotTo(BeEmpty())
		_, err = stream.Append(ctx, AppendRecord{Body: []byte("late"), ContentType: "text/plain"})
		Expect(IsKind(err, ErrorClosed)).To(BeTrue())
		page, err = stream.Read(ctx, client.Beginning(), ReadOptions{})
		Expect(err).NotTo(HaveOccurred())
		Expect(page.Closed).To(BeTrue())
		deleted, err := stream.Delete(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(deleted).To(BeTrue())
	})

	ginkgo.It("round trips through a running Durable Streams server", func() {
		if os.Getenv("PICOMQ_DS_INTEGRATION") == "" {
			ginkgo.Skip("set PICOMQ_DS_INTEGRATION=1 to run live Durable Streams tests")
		}
		client, err := NewDurableStreams(liveEndpoint("PICOMQ_DS_ENDPOINT"), liveOptions()...)
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
