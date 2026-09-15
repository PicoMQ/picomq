package picomq

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"
)

type GroupConfig struct {
	SessionTimeout    time.Duration
	HeartbeatInterval time.Duration
	RebalanceTimeout  time.Duration
	InstanceID        string
	ClientID          string
	Retry             RetryPolicy
}

func DefaultGroupConfig() GroupConfig {
	return GroupConfig{SessionTimeout: 30 * time.Second, Retry: RetryPolicy{MaxAttempts: math.MaxInt32, InitialBackoff: 100 * time.Millisecond, MaxBackoff: 5 * time.Second, Multiplier: 2}}
}

func (c GroupConfig) joinOptions(memberID string) JoinOptions {
	return JoinOptions{MemberID: memberID, InstanceID: c.InstanceID, ClientID: c.ClientID, SessionTimeout: c.SessionTimeout, RebalanceTimeout: c.RebalanceTimeout}
}

type Assignment struct {
	Generation int32
	Streams    []string
}

type GroupMember struct {
	client       *PicoClient
	group        string
	subscription []string
	config       GroupConfig
	assignments  chan Assignment
	cancel       context.CancelFunc
	done         chan struct{}
	mu           sync.Mutex
	memberID     string
	generation   int32
	current      Assignment
	err          error
}

func (c *PicoClient) NewGroupMember(ctx context.Context, group string, subscription []string, config *GroupConfig) (*GroupMember, error) {
	cfg := DefaultGroupConfig()
	if config != nil {
		cfg = *config
		if cfg.SessionTimeout <= 0 {
			cfg.SessionTimeout = 30 * time.Second
		}
		if cfg.Retry.MaxAttempts == 0 {
			cfg.Retry = DefaultGroupConfig().Retry
		}
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = cfg.SessionTimeout / 3
	}
	if cfg.HeartbeatInterval < time.Millisecond {
		cfg.HeartbeatInterval = time.Millisecond
	}
	m := &GroupMember{client: c, group: group, subscription: append([]string(nil), subscription...), config: cfg, assignments: make(chan Assignment, 1), done: make(chan struct{})}
	joined, err := m.rejoin(ctx, "")
	if err != nil {
		return nil, err
	}
	m.apply(joined)
	loopCtx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel
	go m.run(loopCtx)
	return m, nil
}

func (m *GroupMember) MemberID() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.memberID
}

func (m *GroupMember) Assignment() Assignment {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.current
}

func (m *GroupMember) Assignments() <-chan Assignment { return m.assignments }

func (m *GroupMember) Err() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.err
}

func (m *GroupMember) Commit(ctx context.Context, offsets Offsets) error {
	m.mu.Lock()
	if m.err != nil {
		defer m.mu.Unlock()
		return m.err
	}
	fence := m.fence()
	m.mu.Unlock()
	return m.client.CommitOffsets(ctx, m.group, offsets, &fence)
}

func (m *GroupMember) FetchOffsets(ctx context.Context, streams []string) (Offsets, error) {
	return m.client.FetchOffsets(ctx, m.group, streams)
}

func (m *GroupMember) Leave(ctx context.Context) error {
	m.cancel()
	select {
	case <-m.done:
	case <-ctx.Done():
		return ctx.Err()
	}
	m.mu.Lock()
	failed, memberID := m.err, m.memberID
	m.mu.Unlock()
	if failed != nil {
		return nil
	}
	return m.client.LeaveGroup(ctx, m.group, memberID, m.config.InstanceID)
}

func (m *GroupMember) fence() MemberFence {
	return MemberFence{MemberID: m.memberID, Generation: m.generation, InstanceID: m.config.InstanceID}
}

func (m *GroupMember) run(ctx context.Context) {
	defer close(m.done)
	defer close(m.assignments)
	attempt := 0
	for {
		if sleepCtx(ctx, m.config.HeartbeatInterval) != nil {
			return
		}
		m.mu.Lock()
		fence := m.fence()
		m.mu.Unlock()
		err := m.client.Heartbeat(ctx, m.group, fence)
		if err == nil {
			attempt = 0
			continue
		}
		if ctx.Err() != nil {
			return
		}
		var joined GroupMembership
		switch code := errorCode(err); {
		case code == "rebalance_in_progress" || code == "illegal_generation":
			joined, err = m.rejoin(ctx, fence.MemberID)
		case code == "unknown_member":
			joined, err = m.rejoin(ctx, "")
		case retryable(err):
			delay, again := m.config.Retry.delay(attempt)
			if again {
				attempt++
				if sleepCtx(ctx, delay) != nil {
					return
				}
				continue
			}
		}
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			m.mu.Lock()
			m.err = err
			m.mu.Unlock()
			return
		}
		attempt = 0
		m.apply(joined)
	}
}

func (m *GroupMember) apply(joined GroupMembership) {
	m.mu.Lock()
	m.memberID, m.generation = joined.MemberID, joined.Generation
	m.current = Assignment{Generation: joined.Generation, Streams: joined.Assignment}
	current := m.current
	m.mu.Unlock()
	select {
	case <-m.assignments:
	default:
	}
	m.assignments <- current
}

func (m *GroupMember) rejoin(ctx context.Context, memberID string) (GroupMembership, error) {
	attempt := 0
	for {
		joined, err := m.client.JoinGroup(ctx, m.group, m.subscription, m.config.joinOptions(memberID))
		if err == nil {
			return joined, nil
		}
		if errorCode(err) == "unknown_member" {
			memberID = ""
			continue
		}
		if !retryable(err) {
			return GroupMembership{}, err
		}
		delay, again := m.config.Retry.delay(attempt)
		if !again {
			return GroupMembership{}, err
		}
		attempt++
		if err := sleepCtx(ctx, delay); err != nil {
			return GroupMembership{}, err
		}
	}
}

func errorCode(err error) string {
	var target *ClientError
	if errors.As(err, &target) {
		return target.Code
	}
	return ""
}
