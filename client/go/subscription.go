package picomq

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

type Subscription struct {
	core     *coreClient
	protocol Protocol
	name     string
	offset   string
	options  SubscribeOptions
	ctx      context.Context
	cancel   context.CancelFunc

	mu          sync.Mutex
	response    *http.Response
	scanner     *bufio.Scanner
	encoding    string
	lastEventID string
	drops       int
	closed      bool
}

func newSubscription(ctx context.Context, core *coreClient, name, from string, options SubscribeOptions) *Subscription {
	if ctx == nil {
		ctx = context.Background()
	}
	lifetime, cancel := context.WithCancel(ctx)
	if options.ReconnectDelay <= 0 {
		options.ReconnectDelay = time.Second
	}
	if options.MaxReconnectDelay <= 0 {
		options.MaxReconnectDelay = 30 * time.Second
	}
	if options.MaxEventBytes <= 0 {
		options.MaxEventBytes = 16 << 20
	}
	return &Subscription{core: core, protocol: core.protocol, name: name, offset: from, options: options, ctx: lifetime, cancel: cancel}
}

func (c *PicoClient) Subscribe(ctx context.Context, name, from string, options SubscribeOptions) *Subscription {
	return newSubscription(ctx, c.core, name, from, options)
}

func (c *DurableStreamsClient) Subscribe(ctx context.Context, name, from string, options SubscribeOptions) *Subscription {
	return newSubscription(ctx, c.core, name, from, options)
}

func (s *Subscription) Next() (Event, error) {
	for {
		if s.isClosed() {
			return Event{}, io.EOF
		}
		if err := s.ctx.Err(); err != nil {
			return Event{}, err
		}
		if s.scanner == nil {
			if err := s.open(); err != nil {
				if retryErr := s.recover(err); retryErr != nil {
					return Event{}, retryErr
				}
				continue
			}
		}
		event, found, err := s.readEvent()
		if err != nil {
			s.dropResponse()
			if retryErr := s.recover(err); retryErr != nil {
				return Event{}, retryErr
			}
			continue
		}
		if !found {
			s.dropResponse()
			if retryErr := s.recover(io.EOF); retryErr != nil {
				return Event{}, retryErr
			}
			continue
		}
		s.drops = 0
		decoded, err := s.decode(event)
		if err != nil {
			return Event{}, err
		}
		if decoded.ID != "" {
			s.lastEventID = decoded.ID
		}
		if decoded.Type == EventControl {
			s.offset = decoded.Next
			if decoded.Closed {
				s.Close()
			}
		}
		return decoded, nil
	}
}

type rawEvent struct{ kind, id, data string }

func (s *Subscription) readEvent() (rawEvent, bool, error) {
	var event rawEvent
	var data []string
	dataBytes := 0
	for s.scanner.Scan() {
		line := s.scanner.Text()
		if line == "" {
			if event.kind == "" && event.id == "" && len(data) == 0 {
				continue
			}
			if event.kind == "" {
				event.kind = "message"
			}
			event.data = strings.Join(data, "\n")
			return event, true, nil
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		field, value, ok := strings.Cut(line, ":")
		if !ok {
			field, value = line, ""
		}
		value = strings.TrimPrefix(value, " ")
		switch field {
		case "event":
			event.kind = value
		case "id":
			if !strings.ContainsRune(value, '\x00') {
				event.id = value
			}
		case "data":
			if len(data) > 0 {
				dataBytes++
			}
			dataBytes += len(value)
			if dataBytes > s.options.MaxEventBytes {
				return rawEvent{}, false, &ClientError{Kind: ErrorOther, Code: "sse_event_too_large", Message: "SSE event exceeds MaxEventBytes"}
			}
			data = append(data, value)
		}
	}
	if err := s.scanner.Err(); err != nil {
		if s.ctx.Err() != nil {
			return rawEvent{}, false, s.ctx.Err()
		}
		return rawEvent{}, false, &ClientError{Kind: ErrorTransport, Code: "sse_read", Message: err.Error(), Cause: err}
	}
	return rawEvent{}, false, nil
}

func (s *Subscription) open() error {
	query := url.Values{"live": {"sse"}}
	if s.protocol == ProtocolPico && s.lastEventID == "" {
		query.Set("seq", s.offset)
	} else if s.protocol == ProtocolDS {
		query.Set("offset", s.offset)
	}
	headers := make(http.Header)
	headers.Set("Accept", "text/event-stream")
	if s.lastEventID != "" {
		headers.Set("Last-Event-ID", s.lastEventID)
	}
	response, err := s.core.send(s.ctx, wireRequest{method: http.MethodGet, url: s.core.streamURL(s.name, query), headers: headers})
	if err != nil {
		return err
	}
	if response.StatusCode != http.StatusOK {
		if s.protocol == ProtocolPico {
			_, err = expectPico(response, http.StatusOK)
		} else {
			_, err = expectDS(response, http.StatusOK)
		}
		return err
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		response.Body.Close()
		return io.EOF
	}
	s.response = response
	s.encoding = response.Header.Get("Stream-SSE-Data-Encoding")
	s.scanner = bufio.NewScanner(response.Body)
	s.scanner.Buffer(make([]byte, 4096), s.options.MaxEventBytes)
	s.mu.Unlock()
	return nil
}

func (s *Subscription) decode(raw rawEvent) (Event, error) {
	event := Event{ID: raw.id, Raw: []byte(raw.data)}
	if raw.kind == "control" {
		event.Type = EventControl
		if s.protocol == ProtocolPico {
			var value struct {
				Next     json.Number `json:"next_seq"`
				UpToDate bool        `json:"up_to_date"`
				Closed   bool        `json:"closed"`
			}
			decoder := json.NewDecoder(strings.NewReader(raw.data))
			decoder.UseNumber()
			if err := decoder.Decode(&value); err != nil {
				return Event{}, invalidResponse(fmt.Errorf("invalid SSE control: %w", err))
			}
			event.Next, event.UpToDate, event.Closed = value.Next.String(), value.UpToDate, value.Closed
		} else {
			var value struct {
				Next     string `json:"streamNextOffset"`
				UpToDate bool   `json:"upToDate"`
				Closed   bool   `json:"streamClosed"`
			}
			if err := json.Unmarshal([]byte(raw.data), &value); err != nil {
				return Event{}, invalidResponse(fmt.Errorf("invalid SSE control: %w", err))
			}
			event.Next, event.UpToDate, event.Closed = value.Next, value.UpToDate, value.Closed
		}
		return event, nil
	}
	if raw.kind != "data" && raw.kind != "message" {
		return Event{}, invalidResponse(fmt.Errorf("unsupported SSE event type %q", raw.kind))
	}
	event.Type = EventData
	if s.protocol == ProtocolPico {
		var rows []struct {
			Seq       json.Number       `json:"seq"`
			Timestamp int64             `json:"timestamp"`
			Key       *string           `json:"key"`
			Key64     *string           `json:"key_b64"`
			Headers   map[string]string `json:"headers"`
			Body      string            `json:"body"`
			Body64    string            `json:"body_b64"`
		}
		decoder := json.NewDecoder(strings.NewReader(raw.data))
		decoder.UseNumber()
		if err := decoder.Decode(&rows); err != nil {
			return Event{}, invalidResponse(fmt.Errorf("invalid SSE data: %w", err))
		}
		for _, row := range rows {
			value := []byte(row.Body)
			if row.Body64 != "" {
				decoded, err := base64.StdEncoding.DecodeString(row.Body64)
				if err != nil {
					return Event{}, invalidResponse(err)
				}
				value = decoded
			}
			var key []byte
			if row.Key64 != nil {
				decodedKey, decodeErr := base64.StdEncoding.DecodeString(*row.Key64)
				if decodeErr != nil {
					return Event{}, invalidResponse(decodeErr)
				}
				key = decodedKey
			} else if row.Key != nil {
				key = []byte(*row.Key)
			}
			event.Records = append(event.Records, Record{Position: row.Seq.String(), Timestamp: row.Timestamp, Key: key, Headers: row.Headers, Body: value})
		}
	} else {
		value := []byte(raw.data)
		if s.encoding == "base64" {
			decoded, err := base64.StdEncoding.DecodeString(raw.data)
			if err != nil {
				return Event{}, invalidResponse(err)
			}
			value = decoded
		}
		event.Records = []Record{{Headers: map[string]string{}, Body: value}}
	}
	return event, nil
}

func (s *Subscription) recover(cause error) error {
	if errors.Is(cause, context.Canceled) || errors.Is(cause, context.DeadlineExceeded) {
		return cause
	}
	if s.options.DisableReconnect {
		return cause
	}
	s.drops++
	if s.options.MaxReconnectAttempts > 0 && s.drops > s.options.MaxReconnectAttempts {
		return cause
	}
	delay := s.options.ReconnectDelay
	for i := 1; i < s.drops; i++ {
		if delay >= s.options.MaxReconnectDelay/2 {
			delay = s.options.MaxReconnectDelay
			break
		}
		delay *= 2
	}
	timer := time.NewTimer(delay)
	select {
	case <-s.ctx.Done():
		if !timer.Stop() {
			<-timer.C
		}
		return s.ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (s *Subscription) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.cancel()
	response := s.response
	s.response = nil
	s.mu.Unlock()
	if response != nil {
		return response.Body.Close()
	}
	return nil
}

func (s *Subscription) isClosed() bool { s.mu.Lock(); defer s.mu.Unlock(); return s.closed }
func (s *Subscription) dropResponse() {
	s.mu.Lock()
	response := s.response
	s.response = nil
	s.scanner = nil
	s.mu.Unlock()
	if response != nil {
		response.Body.Close()
	}
}
