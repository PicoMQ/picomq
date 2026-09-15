package picomq

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func readJSON(r *http.Request) map[string]any {
	data, err := body(&http.Response{Body: r.Body}, maxResponseBody)
	Expect(err).NotTo(HaveOccurred())
	var decoded map[string]any
	Expect(json.Unmarshal(data, &decoded)).To(Succeed())
	return decoded
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func groupFailure(w http.ResponseWriter, status int, code string) {
	writeJSON(w, status, map[string]any{"error": code, "message": code})
}

func awaitGeneration(member *GroupMember, after int32) Assignment {
	deadline := time.After(10 * time.Second)
	for {
		select {
		case assignment, ok := <-member.Assignments():
			if !ok {
				ginkgo.Fail("assignments closed before generation advanced; err=" + errorCode(member.Err()))
			}
			if assignment.Generation > after {
				return assignment
			}
		case <-deadline:
			ginkgo.Fail("no rebalance observed")
		}
	}
}

var _ = ginkgo.Describe("Pico groups", func() {
	ginkgo.It("speaks the group wire contract", func() {
		var calls atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch calls.Add(1) {
			case 1:
				Expect(r.Method).To(Equal(http.MethodPost))
				Expect(r.URL.EscapedPath()).To(Equal("/_groups/orders%2Feu/members"))
				Expect(r.Header.Get("Content-Type")).To(Equal("application/json"))
				Expect(readJSON(r)).To(Equal(map[string]any{
					"subscription": []any{"/a", "/b"}, "memberId": "m/1", "instanceId": "i-1", "clientId": "test",
					"sessionTimeoutMs": float64(6000), "rebalanceTimeoutMs": float64(9000),
				}))
				writeJSON(w, http.StatusOK, map[string]any{"memberId": "m/1", "generation": 2, "assignment": []string{"/a"}, "members": []string{"m/1", "m/2"}})
			case 2:
				Expect(r.Method).To(Equal(http.MethodPost))
				Expect(r.URL.EscapedPath()).To(Equal("/_groups/orders%2Feu/members/m%2F1/heartbeat"))
				Expect(readJSON(r)).To(Equal(map[string]any{"generation": float64(2), "instanceId": "i-1"}))
				w.WriteHeader(http.StatusNoContent)
			case 3:
				Expect(r.Method).To(Equal(http.MethodGet))
				Expect(r.URL.EscapedPath()).To(Equal("/_groups/orders%2Feu/members/m%2F1"))
				Expect(r.URL.Query().Get("generation")).To(Equal("2"))
				Expect(r.URL.Query().Get("instanceId")).To(Equal("i-1"))
				writeJSON(w, http.StatusOK, map[string]any{"generation": 2, "assignment": []string{"/a"}})
			case 4:
				Expect(r.Method).To(Equal(http.MethodPut))
				Expect(r.URL.EscapedPath()).To(Equal("/_groups/orders%2Feu/offsets"))
				Expect(readJSON(r)).To(Equal(map[string]any{
					"offsets":  map[string]any{"/a": map[string]any{"position": float64(7), "metadata": "ck"}},
					"memberId": "m/1", "generation": float64(2), "instanceId": "i-1",
				}))
				w.WriteHeader(http.StatusNoContent)
			case 5:
				Expect(readJSON(r)).To(Equal(map[string]any{"offsets": map[string]any{"/b": map[string]any{"position": float64(1)}}}))
				w.WriteHeader(http.StatusNoContent)
			case 6:
				Expect(r.Method).To(Equal(http.MethodGet))
				Expect(r.URL.EscapedPath()).To(Equal("/_groups/orders%2Feu/offsets"))
				Expect(r.URL.Query()["stream"]).To(Equal([]string{"/a", "/b c"}))
				writeJSON(w, http.StatusOK, map[string]any{"offsets": map[string]any{"/a": map[string]any{"position": 7, "metadata": "ck"}, "/b c": map[string]any{"position": 1, "metadata": nil}}})
			case 7:
				Expect(r.Method).To(Equal(http.MethodGet))
				Expect(r.URL.EscapedPath()).To(Equal("/_groups/orders%2Feu"))
				writeJSON(w, http.StatusOK, map[string]any{
					"group": "orders/eu", "state": "Stable", "generation": 2, "protocolType": "consumer",
					"members": []map[string]any{{"memberId": "m/1", "instanceId": nil, "clientId": "test", "subscription": []string{"/a", "/b"}, "assignment": []string{"/a"}}},
				})
			case 8:
				Expect(r.URL.EscapedPath()).To(Equal("/_groups"))
				writeJSON(w, http.StatusOK, map[string]any{"groups": []map[string]any{{"group": "orders/eu", "state": "Stable"}}})
			case 9:
				Expect(r.Method).To(Equal(http.MethodDelete))
				Expect(r.URL.EscapedPath()).To(Equal("/_groups/orders%2Feu/members/m%2F1"))
				Expect(r.URL.Query().Get("instanceId")).To(Equal("i-1"))
				w.WriteHeader(http.StatusNoContent)
			}
		}))
		defer server.Close()
		client, err := NewPico(server.URL)
		Expect(err).NotTo(HaveOccurred())
		ctx := context.Background()

		joined, err := client.JoinGroup(ctx, "orders/eu", []string{"/a", "/b"}, JoinOptions{MemberID: "m/1", InstanceID: "i-1", ClientID: "test", SessionTimeout: 6 * time.Second, RebalanceTimeout: 9 * time.Second})
		Expect(err).NotTo(HaveOccurred())
		Expect(joined).To(Equal(GroupMembership{MemberID: "m/1", Generation: 2, Assignment: []string{"/a"}, Members: []string{"m/1", "m/2"}}))

		fence := MemberFence{MemberID: "m/1", Generation: 2, InstanceID: "i-1"}
		Expect(client.Heartbeat(ctx, "orders/eu", fence)).To(Succeed())
		assignment, err := client.GroupAssignment(ctx, "orders/eu", fence)
		Expect(err).NotTo(HaveOccurred())
		Expect(assignment).To(Equal(GroupAssignment{Generation: 2, Assignment: []string{"/a"}}))

		Expect(client.CommitOffsets(ctx, "orders/eu", Offsets{"/a": {Position: 7, Metadata: "ck"}}, &fence)).To(Succeed())
		Expect(client.CommitOffsets(ctx, "orders/eu", Offsets{"/b": {Position: 1}}, nil)).To(Succeed())
		offsets, err := client.FetchOffsets(ctx, "orders/eu", []string{"/a", "/b c"})
		Expect(err).NotTo(HaveOccurred())
		Expect(offsets).To(Equal(Offsets{"/a": {Position: 7, Metadata: "ck"}, "/b c": {Position: 1}}))

		described, err := client.DescribeGroup(ctx, "orders/eu")
		Expect(err).NotTo(HaveOccurred())
		Expect(described).To(Equal(GroupDescription{Group: "orders/eu", State: "Stable", Generation: 2, ProtocolType: "consumer", Members: []MemberDescription{{MemberID: "m/1", ClientID: "test", Subscription: []string{"/a", "/b"}, Assignment: []string{"/a"}}}}))
		groups, err := client.ListGroups(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(groups).To(Equal([]GroupSummary{{Group: "orders/eu", State: "Stable"}}))

		Expect(client.LeaveGroup(ctx, "orders/eu", "m/1", "i-1")).To(Succeed())
		Expect(calls.Load()).To(Equal(int32(9)))
	})

	ginkgo.It("maps group errors and rejects malformed responses", func() {
		var calls atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch calls.Add(1) {
			case 1:
				groupFailure(w, http.StatusConflict, "illegal_generation")
			case 2:
				writeJSON(w, http.StatusOK, map[string]any{"generation": 1})
			default:
				groupFailure(w, http.StatusServiceUnavailable, "unavailable")
			}
		}))
		defer server.Close()
		client, err := NewPico(server.URL, WithRetryPolicy(RetryAttempts(3)))
		Expect(err).NotTo(HaveOccurred())
		ctx := context.Background()

		err = client.CommitOffsets(ctx, "g", Offsets{}, &MemberFence{MemberID: "m", Generation: 0})
		var clientErr *ClientError
		Expect(errors.As(err, &clientErr)).To(BeTrue())
		Expect(clientErr.Kind).To(Equal(ErrorConflict))
		Expect(clientErr.Code).To(Equal("illegal_generation"))
		Expect(clientErr.Status).To(Equal(http.StatusConflict))

		_, err = client.JoinGroup(ctx, "g", []string{"/a"}, JoinOptions{})
		Expect(IsKind(err, ErrorInvalidResponse)).To(BeTrue())

		before := calls.Load()
		Expect(client.Heartbeat(ctx, "g", MemberFence{MemberID: "m", Generation: 1})).NotTo(Succeed())
		_, err = client.JoinGroup(ctx, "g", []string{"/a"}, JoinOptions{})
		Expect(err).To(HaveOccurred())
		Expect(calls.Load()-before).To(Equal(int32(2)), "joins and heartbeats are not retried")

		before = calls.Load()
		_, err = client.ListGroups(ctx)
		Expect(err).To(HaveOccurred())
		Expect(calls.Load()-before).To(Equal(int32(3)), "list retries with the client policy")
	})

	ginkgo.It("rejoins on rebalance and publishes the new assignment", func() {
		var mu sync.Mutex
		var joins []map[string]any
		var heartbeats, leaves atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch {
			case r.Method == http.MethodPost && r.URL.Path == "/_groups/g/members":
				request := readJSON(r)
				mu.Lock()
				joins = append(joins, request)
				n := len(joins)
				mu.Unlock()
				switch n {
				case 1:
					writeJSON(w, http.StatusOK, map[string]any{"memberId": "m-1", "generation": 1, "assignment": []string{"/a", "/b"}, "members": []string{"m-1"}})
				case 2:
					writeJSON(w, http.StatusOK, map[string]any{"memberId": "m-1", "generation": 2, "assignment": []string{"/a"}, "members": []string{"m-1", "m-2"}})
				default:
					writeJSON(w, http.StatusOK, map[string]any{"memberId": "m-9", "generation": 3, "assignment": []string{"/b"}, "members": []string{"m-9"}})
				}
			case r.Method == http.MethodPost && r.URL.Path == "/_groups/g/members/m-1/heartbeat":
				switch heartbeats.Add(1) {
				case 1:
					w.WriteHeader(http.StatusNoContent)
				case 2:
					groupFailure(w, http.StatusConflict, "rebalance_in_progress")
				case 3:
					groupFailure(w, http.StatusServiceUnavailable, "unavailable")
				case 4:
					groupFailure(w, http.StatusNotFound, "unknown_member")
				default:
					w.WriteHeader(http.StatusNoContent)
				}
			case r.Method == http.MethodPost && r.URL.Path == "/_groups/g/members/m-9/heartbeat":
				w.WriteHeader(http.StatusNoContent)
			case r.Method == http.MethodPut && r.URL.Path == "/_groups/g/offsets":
				Expect(readJSON(r)).To(Equal(map[string]any{"offsets": map[string]any{"/b": map[string]any{"position": float64(4)}}, "memberId": "m-9", "generation": float64(3)}))
				w.WriteHeader(http.StatusNoContent)
			case r.Method == http.MethodDelete && r.URL.Path == "/_groups/g/members/m-9":
				leaves.Add(1)
				w.WriteHeader(http.StatusNoContent)
			default:
				ginkgo.Fail("unexpected request " + r.Method + " " + r.URL.String())
			}
		}))
		defer server.Close()
		client, err := NewPico(server.URL)
		Expect(err).NotTo(HaveOccurred())
		ctx := context.Background()

		config := GroupConfig{SessionTimeout: 300 * time.Millisecond, HeartbeatInterval: 5 * time.Millisecond, ClientID: "go-test", Retry: RetryPolicy{MaxAttempts: 3, Multiplier: 1}}
		member, err := client.NewGroupMember(ctx, "g", []string{"/a", "/b"}, &config)
		Expect(err).NotTo(HaveOccurred())
		Expect(member.MemberID()).To(Equal("m-1"))
		Expect(member.Assignment()).To(Equal(Assignment{Generation: 1, Streams: []string{"/a", "/b"}}))
		Expect(<-member.Assignments()).To(Equal(Assignment{Generation: 1, Streams: []string{"/a", "/b"}}))

		Expect(awaitGeneration(member, 1)).To(Equal(Assignment{Generation: 2, Streams: []string{"/a"}}))
		Expect(member.MemberID()).To(Equal("m-1"))
		Expect(awaitGeneration(member, 2)).To(Equal(Assignment{Generation: 3, Streams: []string{"/b"}}))
		Expect(member.MemberID()).To(Equal("m-9"))
		Expect(member.Err()).NotTo(HaveOccurred())

		mu.Lock()
		Expect(joins).To(HaveLen(3))
		Expect(joins[0]).NotTo(HaveKey("memberId"))
		Expect(joins[0]["clientId"]).To(Equal("go-test"))
		Expect(joins[0]["sessionTimeoutMs"]).To(Equal(float64(300)))
		Expect(joins[1]["memberId"]).To(Equal("m-1"), "rebalance rejoins keep the member id")
		Expect(joins[2]).NotTo(HaveKey("memberId"), "unknown_member rejoins with a fresh id")
		mu.Unlock()

		Expect(member.Commit(ctx, Offsets{"/b": {Position: 4}})).To(Succeed())
		Expect(member.Leave(ctx)).To(Succeed())
		Expect(leaves.Load()).To(Equal(int32(1)))
		_, open := <-member.Assignments()
		Expect(open).To(BeFalse())
	})

	ginkgo.It("fails the session on a fatal heartbeat error and skips leave", func() {
		var leaves atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch {
			case r.Method == http.MethodPost && r.URL.Path == "/_groups/g/members":
				writeJSON(w, http.StatusOK, map[string]any{"memberId": "m-1", "generation": 1, "assignment": []string{"/a"}, "members": []string{"m-1"}})
			case r.Method == http.MethodPost && r.URL.Path == "/_groups/g/members/m-1/heartbeat":
				groupFailure(w, http.StatusForbidden, "fenced")
			case r.Method == http.MethodDelete:
				leaves.Add(1)
				w.WriteHeader(http.StatusNoContent)
			}
		}))
		defer server.Close()
		client, err := NewPico(server.URL)
		Expect(err).NotTo(HaveOccurred())
		ctx := context.Background()

		config := GroupConfig{SessionTimeout: 300 * time.Millisecond, HeartbeatInterval: 5 * time.Millisecond, Retry: NoRetries()}
		member, err := client.NewGroupMember(ctx, "g", []string{"/a"}, &config)
		Expect(err).NotTo(HaveOccurred())
		for range member.Assignments() {
		}
		Expect(errorCode(member.Err())).To(Equal("fenced"))
		Expect(errorCode(member.Commit(ctx, Offsets{"/a": {Position: 1}}))).To(Equal("fenced"))
		Expect(member.Leave(ctx)).To(Succeed())
		Expect(leaves.Load()).To(BeZero())
	})

	ginkgo.It("fails when retryable heartbeat errors exhaust the policy", func() {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/_groups/g/members" {
				writeJSON(w, http.StatusOK, map[string]any{"memberId": "m-1", "generation": 1, "assignment": []string{"/a"}, "members": []string{"m-1"}})
				return
			}
			groupFailure(w, http.StatusServiceUnavailable, "unavailable")
		}))
		defer server.Close()
		client, err := NewPico(server.URL)
		Expect(err).NotTo(HaveOccurred())

		config := GroupConfig{SessionTimeout: 300 * time.Millisecond, HeartbeatInterval: 5 * time.Millisecond, Retry: RetryPolicy{MaxAttempts: 2, Multiplier: 1}}
		member, err := client.NewGroupMember(context.Background(), "g", []string{"/a"}, &config)
		Expect(err).NotTo(HaveOccurred())
		for range member.Assignments() {
		}
		Expect(retryable(member.Err())).To(BeTrue())
	})

	ginkgo.It("retries the initial join and surfaces terminal join errors", func() {
		var joins atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			switch joins.Add(1) {
			case 1:
				groupFailure(w, http.StatusServiceUnavailable, "unavailable")
			case 2:
				writeJSON(w, http.StatusOK, map[string]any{"memberId": "m-1", "generation": 1, "assignment": []string{"/a"}, "members": []string{"m-1"}})
			default:
				groupFailure(w, http.StatusBadRequest, "inconsistent_protocol")
			}
		}))
		defer server.Close()
		client, err := NewPico(server.URL)
		Expect(err).NotTo(HaveOccurred())

		config := GroupConfig{SessionTimeout: time.Second, Retry: RetryPolicy{MaxAttempts: 3, Multiplier: 1}}
		member, err := client.NewGroupMember(context.Background(), "g", []string{"/a"}, &config)
		Expect(err).NotTo(HaveOccurred())
		Expect(member.MemberID()).To(Equal("m-1"))
		Expect(member.Leave(context.Background())).To(Succeed())

		_, err = client.NewGroupMember(context.Background(), "g", []string{"/a"}, &config)
		Expect(errorCode(err)).To(Equal("inconsistent_protocol"))
	})
})
