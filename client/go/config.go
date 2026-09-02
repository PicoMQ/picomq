package picomq

import (
	"errors"
	"math/rand"
	"net/http"
	"time"
)

func durationSecondsCeil(value time.Duration) int64 {
	return int64((value-1)/time.Second) + 1
}

type RetryPolicy struct {
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	Multiplier     float64
}

func NoRetries() RetryPolicy { return RetryPolicy{MaxAttempts: 1, Multiplier: 1} }

func RetryAttempts(attempts int) RetryPolicy {
	return RetryPolicy{MaxAttempts: attempts, InitialBackoff: 100 * time.Millisecond, MaxBackoff: 30 * time.Second, Multiplier: 2}
}

func (p RetryPolicy) normalized() RetryPolicy {
	if p.MaxAttempts < 1 {
		p.MaxAttempts = 1
	}
	if p.Multiplier < 1 {
		p.Multiplier = 1
	}
	if p.InitialBackoff < 0 {
		p.InitialBackoff = 0
	}
	if p.MaxBackoff < 0 {
		p.MaxBackoff = 0
	}
	return p
}

func (p RetryPolicy) delay(attempt int) (time.Duration, bool) {
	p = p.normalized()
	if attempt+1 >= p.MaxAttempts {
		return 0, false
	}
	d := float64(p.InitialBackoff)
	for i := 0; i < attempt; i++ {
		d *= p.Multiplier
	}
	if p.MaxBackoff > 0 && time.Duration(d) > p.MaxBackoff {
		d = float64(p.MaxBackoff)
	}
	if d <= 0 {
		return 0, true
	}
	return time.Duration(d/2 + rand.Float64()*d/2), true
}

type Option func(*clientConfig) error

type clientConfig struct {
	token      string
	httpClient *http.Client
	retry      RetryPolicy
	h2c        bool
}

func WithToken(token string) Option {
	return func(c *clientConfig) error { c.token = token; return nil }
}

func WithHTTPClient(client *http.Client) Option {
	return func(c *clientConfig) error {
		if client == nil {
			return errors.New("picomq: HTTP client cannot be nil")
		}
		c.httpClient = client
		return nil
	}
}

func WithRetryPolicy(policy RetryPolicy) Option {
	return func(c *clientConfig) error { c.retry = policy.normalized(); return nil }
}

// WithH2C enables cleartext HTTP/2 prior knowledge. It cannot be combined with
// WithHTTPClient because it replaces the HTTP transport.
func WithH2C() Option {
	return func(c *clientConfig) error { c.h2c = true; return nil }
}
