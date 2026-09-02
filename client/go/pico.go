package picomq

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

func (c *PicoClient) Stream(name string) *PicoStream { return &PicoStream{client: c, name: name} }

func (c *PicoClient) Create(ctx context.Context, name, contentType string, ttl time.Duration) (created bool, err error) {
	err = c.core.run(ctx, func() error {
		headers := make(http.Header)
		headers.Set("Content-Type", contentType)
		if ttl > 0 {
			headers.Set("Pico-TTL", strconv.FormatInt(int64(ttl/time.Second), 10))
		}
		response, callErr := c.core.send(ctx, wireRequest{method: http.MethodPut, url: c.core.streamURL(name, nil), headers: headers})
		if callErr != nil {
			return callErr
		}
		created = response.StatusCode == http.StatusCreated
		_, callErr = expectPico(response, http.StatusOK, http.StatusCreated)
		return callErr
	})
	return
}

func (c *PicoClient) Head(ctx context.Context, name string) (info *StreamInfo, err error) {
	err = c.core.run(ctx, func() error {
		response, callErr := c.core.send(ctx, wireRequest{method: http.MethodHead, url: c.core.streamURL(name, nil)})
		if callErr != nil {
			return callErr
		}
		if response.StatusCode == http.StatusNotFound {
			response.Body.Close()
			info = nil
			return nil
		}
		if _, callErr = expectPico(response, http.StatusOK); callErr != nil {
			return callErr
		}
		info = &StreamInfo{Name: name, ContentType: response.Header.Get("Content-Type"), Start: defaultString(response.Header.Get("Pico-Start-Seq"), "0"), Next: defaultString(response.Header.Get("Pico-Next-Seq"), "0"), Closed: headerBool(response.Header, "Pico-Closed"), TTL: time.Duration(headerUint(response.Header, "Pico-TTL")) * time.Second, ExpiresAt: response.Header.Get("Pico-Expires-At")}
		return nil
	})
	return
}

func (c *PicoClient) Append(ctx context.Context, name string, records []AppendRecord) (AppendAck, error) {
	return c.append(ctx, name, records, nil)
}

func (c *PicoClient) AppendAs(ctx context.Context, name string, records []AppendRecord, producer ProducerRef) (ProducerAck, error) {
	headers := make(http.Header)
	headers.Set("Pico-Producer-Id", producer.ID)
	headers.Set("Pico-Producer-Epoch", strconv.FormatUint(producer.Epoch, 10))
	headers.Set("Pico-Producer-Seq", strconv.FormatUint(producer.Seq, 10))
	ack, responseHeaders, err := c.appendWithHeaders(ctx, name, records, headers)
	if err != nil {
		return ProducerAck{}, err
	}
	applied := responseHeaders.Get("Pico-Start-Seq") != ""
	return ProducerAck{Applied: applied, Duplicate: !applied && len(records) > 0, Ack: ack}, nil
}

func (c *PicoClient) append(ctx context.Context, name string, records []AppendRecord, headers http.Header) (AppendAck, error) {
	ack, _, err := c.appendWithHeaders(ctx, name, records, headers)
	return ack, err
}

func (c *PicoClient) appendWithHeaders(ctx context.Context, name string, records []AppendRecord, headers http.Header) (AppendAck, http.Header, error) {
	payload, err := encodeBatch(records)
	if err != nil {
		return AppendAck{}, nil, &ClientError{Kind: ErrorBadRequest, Code: "invalid_record", Message: err.Error(), Cause: err}
	}
	if headers == nil {
		headers = make(http.Header)
	}
	headers.Set("Content-Type", "application/vnd.picomq.batch")
	response, err := c.core.send(ctx, wireRequest{method: http.MethodPost, url: c.core.streamURL(name, nil), headers: headers, body: payload})
	if err != nil {
		return AppendAck{}, nil, err
	}
	if _, err = expectPico(response, http.StatusOK); err != nil {
		return AppendAck{}, nil, err
	}
	next := defaultString(response.Header.Get("Pico-Next-Seq"), "0")
	return AppendAck{Start: defaultString(response.Header.Get("Pico-Start-Seq"), next), Next: next, Timestamp: int64(headerUint(response.Header, "Pico-Timestamp"))}, response.Header, nil
}

func (c *PicoClient) Read(ctx context.Context, name, from string, options ReadOptions) (page ReadPage, err error) {
	err = c.core.run(ctx, func() error {
		query := url.Values{"format": {"binary"}, "seq": {from}}
		if options.Limits.Count > 0 {
			query.Set("count", strconv.FormatUint(options.Limits.Count, 10))
		}
		if options.Limits.Bytes > 0 {
			query.Set("bytes", strconv.FormatUint(options.Limits.Bytes, 10))
		}
		if options.Live == LiveLongPoll {
			query.Set("live", "long-poll")
		}
		response, callErr := c.core.send(ctx, wireRequest{method: http.MethodGet, url: c.core.streamURL(name, query)})
		if callErr != nil {
			return callErr
		}
		expected := []int{http.StatusOK}
		if options.Live == LiveLongPoll {
			expected = append(expected, http.StatusNoContent)
		}
		data, callErr := expectPico(response, expected...)
		if callErr != nil {
			return callErr
		}
		page = ReadPage{Next: defaultString(response.Header.Get("Pico-Next-Seq"), from), UpToDate: headerBool(response.Header, "Pico-Up-To-Date") || response.StatusCode == http.StatusNoContent, Closed: headerBool(response.Header, "Pico-Closed")}
		if len(data) > 0 {
			page.Records, callErr = decodeBatch(data)
			if callErr != nil {
				return invalidResponse(callErr)
			}
		}
		return nil
	})
	return
}

func (c *PicoClient) List(ctx context.Context, prefix string, limit uint64) (listing StreamListing, err error) {
	err = c.core.run(ctx, func() error {
		query := url.Values{"prefix": {prefix}}
		if limit > 0 {
			query.Set("limit", strconv.FormatUint(limit, 10))
		}
		response, callErr := c.core.send(ctx, wireRequest{method: http.MethodGet, url: c.core.streamURL("/", query)})
		if callErr != nil {
			return callErr
		}
		data, callErr := expectPico(response, http.StatusOK)
		if callErr != nil {
			return callErr
		}
		// The server uses snake_case; decode explicitly to keep the public model independent.
		var raw struct {
			Streams []struct {
				Name        string `json:"name"`
				ContentType string `json:"content_type"`
				Start       uint64 `json:"start_seq"`
				Next        uint64 `json:"next_seq"`
				Closed      bool   `json:"closed"`
				TTL         uint64 `json:"ttl"`
				ExpiresAt   string `json:"expires_at"`
			} `json:"streams"`
			HasMore bool `json:"has_more"`
		}
		if callErr = json.Unmarshal(data, &raw); callErr != nil {
			return invalidResponse(callErr)
		}
		listing = StreamListing{Streams: make([]StreamInfo, 0, len(raw.Streams))}
		listing.HasMore = raw.HasMore
		for _, item := range raw.Streams {
			listing.Streams = append(listing.Streams, StreamInfo{Name: item.Name, ContentType: item.ContentType, Start: strconv.FormatUint(item.Start, 10), Next: strconv.FormatUint(item.Next, 10), Closed: item.Closed, TTL: time.Duration(item.TTL) * time.Second, ExpiresAt: item.ExpiresAt})
		}
		return nil
	})
	return
}

func (c *PicoClient) Trim(ctx context.Context, name string, seq uint64) (start string, err error) {
	err = c.core.run(ctx, func() error {
		headers := make(http.Header)
		headers.Set("Pico-Trim-Seq", strconv.FormatUint(seq, 10))
		response, callErr := c.core.send(ctx, wireRequest{method: http.MethodPost, url: c.core.streamURL(name, nil), headers: headers})
		if callErr != nil {
			return callErr
		}
		_, callErr = expectPico(response, http.StatusOK)
		start = defaultString(response.Header.Get("Pico-Start-Seq"), "0")
		return callErr
	})
	return
}

func (c *PicoClient) Close(ctx context.Context, name string) (next string, err error) {
	err = c.core.run(ctx, func() error {
		headers := make(http.Header)
		headers.Set("Pico-Closed", "true")
		response, e := c.core.send(ctx, wireRequest{method: http.MethodPost, url: c.core.streamURL(name, nil), headers: headers})
		if e != nil {
			return e
		}
		_, e = expectPico(response, http.StatusOK)
		next = defaultString(response.Header.Get("Pico-Next-Seq"), "0")
		return e
	})
	return
}

func (c *PicoClient) Delete(ctx context.Context, name string) (deleted bool, err error) {
	err = c.core.run(ctx, func() error {
		response, e := c.core.send(ctx, wireRequest{method: http.MethodDelete, url: c.core.streamURL(name, nil)})
		if e != nil {
			return e
		}
		if response.StatusCode == http.StatusNotFound {
			response.Body.Close()
			deleted = false
			return nil
		}
		_, e = expectPico(response, http.StatusNoContent)
		deleted = e == nil
		return e
	})
	return
}

func defaultString(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
