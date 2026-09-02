package picomq

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"golang.org/x/net/http2"
)

const (
	maxRedirects    = 5
	maxErrorBody    = 1 << 20
	maxResponseBody = 64 << 20
)

type coreClient struct {
	protocol       Protocol
	baseURL        *url.URL
	http           *http.Client
	token          string
	retry          RetryPolicy
	ownedTransport bool
}

type PicoClient struct{ core *coreClient }
type DurableStreamsClient struct{ core *coreClient }

// Client is the protocol-neutral stream API. Protocol-specific constructors
// expose additional operations such as Pico listing, trimming, and producers.
type Client interface {
	Protocol() Protocol
	Beginning() string
	Now() (string, error)
	Create(context.Context, string, string, time.Duration) (bool, error)
	Head(context.Context, string) (*StreamInfo, error)
	Append(context.Context, string, []AppendRecord) (AppendAck, error)
	Read(context.Context, string, string, ReadOptions) (ReadPage, error)
	Subscribe(context.Context, string, string, SubscribeOptions) *Subscription
	List(context.Context, string, uint64) (StreamListing, error)
	Close(context.Context, string) (string, error)
	Delete(context.Context, string) (bool, error)
}

var (
	_ Client = (*PicoClient)(nil)
	_ Client = (*DurableStreamsClient)(nil)
)

// Connect constructs a protocol-neutral client without performing network I/O.
func Connect(protocol Protocol, endpoint string, options ...Option) (Client, error) {
	switch protocol {
	case ProtocolPico:
		return NewPico(endpoint, options...)
	case ProtocolDS:
		return NewDurableStreams(endpoint, options...)
	default:
		return nil, fmt.Errorf("picomq: unsupported protocol %q", protocol)
	}
}

func NewPico(endpoint string, options ...Option) (*PicoClient, error) {
	core, err := newCore(ProtocolPico, endpoint, options...)
	if err != nil {
		return nil, err
	}
	return &PicoClient{core: core}, nil
}

func NewDurableStreams(endpoint string, options ...Option) (*DurableStreamsClient, error) {
	core, err := newCore(ProtocolDS, endpoint, options...)
	if err != nil {
		return nil, err
	}
	return &DurableStreamsClient{core: core}, nil
}

func newCore(protocol Protocol, endpoint string, options ...Option) (*coreClient, error) {
	base, err := url.Parse(strings.TrimRight(endpoint, "/"))
	if err != nil || base.Scheme == "" || base.Host == "" {
		return nil, fmt.Errorf("picomq: invalid endpoint %q", endpoint)
	}
	if base.Scheme != "http" && base.Scheme != "https" {
		return nil, fmt.Errorf("picomq: unsupported endpoint scheme %q", base.Scheme)
	}
	cfg := clientConfig{retry: NoRetries()}
	for _, option := range options {
		if err := option(&cfg); err != nil {
			return nil, err
		}
	}
	owned := cfg.httpClient == nil
	if cfg.h2c && cfg.httpClient != nil {
		return nil, errors.New("picomq: WithH2C cannot be combined with WithHTTPClient")
	}
	if cfg.h2c && base.Scheme != "http" {
		return nil, errors.New("picomq: WithH2C requires an http endpoint")
	}
	if cfg.httpClient == nil {
		if cfg.h2c {
			cfg.httpClient = &http.Client{Transport: &http2.Transport{AllowHTTP: true, DialTLSContext: func(ctx context.Context, network, address string, _ *tls.Config) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, network, address)
			}}}
		} else {
			transport := http.DefaultTransport.(*http.Transport).Clone()
			cfg.httpClient = &http.Client{Transport: transport}
		}
	}
	return &coreClient{protocol: protocol, baseURL: base, http: cfg.httpClient, token: cfg.token, retry: cfg.retry, ownedTransport: owned}, nil
}

func (c *PicoClient) Protocol() Protocol           { return ProtocolPico }
func (c *DurableStreamsClient) Protocol() Protocol { return ProtocolDS }
func (c *PicoClient) Beginning() string            { return "0" }
func (c *DurableStreamsClient) Beginning() string  { return "-1" }
func (c *PicoClient) Now() (string, error) {
	return "", unsupported("the Pico protocol has no now token; use the stream's next position")
}
func (c *DurableStreamsClient) Now() (string, error) { return "now", nil }

func (c *PicoClient) CloseIdleConnections()           { c.core.closeIdleConnections() }
func (c *DurableStreamsClient) CloseIdleConnections() { c.core.closeIdleConnections() }
func (c *coreClient) closeIdleConnections() {
	if c.ownedTransport {
		c.http.CloseIdleConnections()
	}
}

func (c *coreClient) streamURL(name string, query url.Values) string {
	u := *c.baseURL
	path := name
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	u.Path = strings.TrimRight(c.baseURL.Path, "/") + path
	u.RawQuery = query.Encode()
	return u.String()
}

type wireRequest struct {
	method, url string
	headers     http.Header
	body        []byte
}

func (c *coreClient) send(ctx context.Context, request wireRequest) (*http.Response, error) {
	current := request.url
	visited := map[string]struct{}{}
	for hop := 0; hop <= maxRedirects; hop++ {
		if _, ok := visited[current]; ok {
			return nil, &ClientError{Kind: ErrorOther, Code: "redirect_loop", Message: "redirect loop detected"}
		}
		visited[current] = struct{}{}
		req, err := http.NewRequestWithContext(ctx, request.method, current, bytes.NewReader(request.body))
		if err != nil {
			return nil, err
		}
		if request.headers == nil {
			req.Header = make(http.Header)
		} else {
			req.Header = request.headers.Clone()
		}
		if c.token != "" && req.Header.Get("Authorization") == "" {
			req.Header.Set("Authorization", "Bearer "+c.token)
		}
		client := *c.http
		client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
		response, err := client.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, &ClientError{Kind: ErrorTransport, Code: "transport", Message: err.Error(), Cause: err}
		}
		if response.StatusCode != http.StatusTemporaryRedirect && response.StatusCode != http.StatusPermanentRedirect {
			return response, nil
		}
		io.Copy(io.Discard, io.LimitReader(response.Body, maxErrorBody))
		response.Body.Close()
		location := response.Header.Get("Location")
		if location == "" {
			return nil, &ClientError{Kind: ErrorInvalidResponse, Status: response.StatusCode, Code: "redirect_without_location", Message: "redirect response has no Location header"}
		}
		if hop == maxRedirects {
			return nil, &ClientError{Kind: ErrorOther, Code: "too_many_redirects", Message: "too many ownership redirects"}
		}
		next, err := url.Parse(location)
		if err != nil {
			return nil, invalidResponse(err)
		}
		base, _ := url.Parse(current)
		next = base.ResolveReference(next)
		if next.Scheme != "http" && next.Scheme != "https" {
			return nil, &ClientError{Kind: ErrorInvalidResponse, Code: "unsafe_redirect", Message: "ownership redirect uses an unsupported scheme"}
		}
		if base.Scheme == "https" && next.Scheme == "http" {
			return nil, &ClientError{Kind: ErrorInvalidResponse, Code: "unsafe_redirect", Message: "ownership redirect cannot downgrade from HTTPS to HTTP"}
		}
		current = next.String()
	}
	panic("unreachable")
}

func (c *coreClient) run(ctx context.Context, operation func() error) error {
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := operation()
		if err == nil {
			return nil
		}
		delay, again := c.retry.delay(attempt)
		if !again || !retryable(err) {
			return err
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func body(response *http.Response, limit int64) ([]byte, error) {
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("response body exceeds %d bytes", limit)
	}
	return data, nil
}

func headerBool(header http.Header, name string) bool {
	return strings.EqualFold(header.Get(name), "true")
}
func headerUint(header http.Header, name string) uint64 {
	value, _ := strconv.ParseUint(header.Get(name), 10, 64)
	return value
}

func expectPico(response *http.Response, expected ...int) ([]byte, error) {
	for _, status := range expected {
		if response.StatusCode == status {
			return body(response, maxResponseBody)
		}
	}
	data, readErr := body(response, maxErrorBody)
	if readErr != nil {
		return nil, invalidResponse(readErr)
	}
	parsed := struct {
		Error   string  `json:"error"`
		Message string  `json:"message"`
		Next    *uint64 `json:"next_seq"`
	}{}
	_ = json.Unmarshal(data, &parsed)
	code := parsed.Error
	if code == "" {
		code = fmt.Sprintf("http_%d", response.StatusCode)
	}
	message := parsed.Message
	if message == "" {
		message = strings.TrimSpace(string(data))
	}
	if message == "" {
		message = code
	}
	kind := ErrorOther
	switch response.StatusCode {
	case 400:
		kind = ErrorBadRequest
	case 401:
		kind = ErrorUnauthenticated
	case 403:
		if code == "permission_denied" {
			kind = ErrorPermission
		} else {
			kind = ErrorStaleEpoch
		}
	case 404:
		kind = ErrorNotFound
	case 409:
		if headerBool(response.Header, "Pico-Closed") || code == "closed" {
			kind = ErrorClosed
		} else {
			kind = ErrorConflict
		}
	case 410:
		kind = ErrorOffsetGone
	case 412:
		kind = ErrorConflict
	}
	next := ""
	if parsed.Next != nil {
		next = strconv.FormatUint(*parsed.Next, 10)
	}
	return nil, &ClientError{Kind: kind, Status: response.StatusCode, Code: code, Message: message, Next: next}
}

func expectDS(response *http.Response, expected ...int) ([]byte, error) {
	for _, status := range expected {
		if response.StatusCode == status {
			return body(response, maxResponseBody)
		}
	}
	data, readErr := body(response, maxErrorBody)
	if readErr != nil {
		return nil, invalidResponse(readErr)
	}
	kind, code := ErrorOther, "request_failed"
	switch response.StatusCode {
	case 400:
		kind, code = ErrorBadRequest, "bad_request"
	case 401:
		kind, code = ErrorUnauthenticated, "unauthenticated"
	case 403:
		if response.Header.Get("Producer-Epoch") != "" {
			kind, code = ErrorStaleEpoch, "stale_epoch"
		} else {
			kind, code = ErrorPermission, "permission_denied"
		}
	case 404:
		kind, code = ErrorNotFound, "not_found"
	case 409:
		if headerBool(response.Header, "Stream-Closed") {
			kind, code = ErrorClosed, "closed"
		} else {
			kind, code = ErrorConflict, "conflict"
		}
	case 410:
		kind, code = ErrorOffsetGone, "offset_gone"
	}
	message := strings.TrimSpace(string(data))
	if message == "" {
		message = code
	}
	return nil, &ClientError{Kind: kind, Status: response.StatusCode, Code: code, Message: message, Next: response.Header.Get("Stream-Next-Offset")}
}

func cloneRecords(records []AppendRecord) []AppendRecord {
	out := make([]AppendRecord, len(records))
	for i, record := range records {
		var headers map[string][]byte
		if record.Headers != nil {
			headers = make(map[string][]byte, len(record.Headers))
			for name, value := range record.Headers {
				headers[name] = cloneBytes(value)
			}
		}
		out[i] = AppendRecord{Body: append([]byte(nil), record.Body...), Key: cloneBytes(record.Key), Headers: headers, Timestamp: record.Timestamp, ContentType: record.ContentType}
	}
	return out
}

func cloneBytes(value []byte) []byte {
	if value == nil {
		return nil
	}
	return append([]byte(nil), value...)
}
