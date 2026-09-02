package picomq

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

func (c *DurableStreamsClient) Stream(name string) *Stream {
	return &Stream{api: c, name: name, beginning: c.Beginning()}
}

func (c *DurableStreamsClient) Create(ctx context.Context, name, contentType string, ttl time.Duration) (created bool, err error) {
	err = c.core.run(ctx, func() error {
		h := make(http.Header)
		h.Set("Content-Type", contentType)
		if ttl > 0 {
			h.Set("Stream-TTL", strconv.FormatInt(durationSecondsCeil(ttl), 10))
		}
		response, e := c.core.send(ctx, wireRequest{method: http.MethodPut, url: c.core.streamURL(name, nil), headers: h})
		if e != nil {
			return e
		}
		created = response.StatusCode == http.StatusCreated
		_, e = expectDS(response, http.StatusOK, http.StatusCreated)
		return e
	})
	return
}

func (c *DurableStreamsClient) Head(ctx context.Context, name string) (info *StreamInfo, err error) {
	err = c.core.run(ctx, func() error {
		response, e := c.core.send(ctx, wireRequest{method: http.MethodHead, url: c.core.streamURL(name, nil)})
		if e != nil {
			return e
		}
		if response.StatusCode == http.StatusNotFound {
			response.Body.Close()
			info = nil
			return nil
		}
		if _, e = expectDS(response, http.StatusOK); e != nil {
			return e
		}
		info = &StreamInfo{Name: name, ContentType: response.Header.Get("Content-Type"), Start: "-1", Next: response.Header.Get("Stream-Next-Offset"), Closed: headerBool(response.Header, "Stream-Closed"), TTL: time.Duration(headerUint(response.Header, "Stream-TTL")) * time.Second, ExpiresAt: response.Header.Get("Stream-Expires-At")}
		return nil
	})
	return
}

func (c *DurableStreamsClient) Append(ctx context.Context, name string, records []AppendRecord) (AppendAck, error) {
	if len(records) != 1 {
		return AppendAck{}, unsupported("the Durable Streams protocol appends exactly one record per request")
	}
	if len(records[0].Headers) > 0 {
		return AppendAck{}, unsupported("the Durable Streams protocol does not support record headers")
	}
	if records[0].Key != nil {
		return AppendAck{}, unsupported("the Durable Streams protocol does not support record keys")
	}
	h := make(http.Header)
	contentType := records[0].ContentType
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	h.Set("Content-Type", contentType)
	response, err := c.core.send(ctx, wireRequest{method: http.MethodPost, url: c.core.streamURL(name, nil), headers: h, body: records[0].Body})
	if err != nil {
		return AppendAck{}, err
	}
	if _, err = expectDS(response, http.StatusOK, http.StatusNoContent); err != nil {
		return AppendAck{}, err
	}
	next := response.Header.Get("Stream-Next-Offset")
	return AppendAck{Start: next, Next: next}, nil
}

func (c *DurableStreamsClient) Read(ctx context.Context, name, from string, options ReadOptions) (page ReadPage, err error) {
	err = c.core.run(ctx, func() error {
		q := url.Values{"offset": {from}}
		if options.Live == LiveLongPoll {
			q.Set("live", "long-poll")
		}
		response, e := c.core.send(ctx, wireRequest{method: http.MethodGet, url: c.core.streamURL(name, q)})
		if e != nil {
			return e
		}
		data, e := expectDS(response, http.StatusOK, http.StatusNoContent)
		if e != nil {
			return e
		}
		next := defaultString(response.Header.Get("Stream-Next-Offset"), from)
		page = ReadPage{Next: next, UpToDate: headerBool(response.Header, "Stream-Up-To-Date") || response.StatusCode == http.StatusNoContent, Closed: headerBool(response.Header, "Stream-Closed")}
		if len(data) > 0 {
			page.Records = []Record{{Position: next, Headers: map[string]string{}, Body: data}}
		}
		return nil
	})
	return
}

func (c *DurableStreamsClient) List(context.Context, string, uint64) (StreamListing, error) {
	return StreamListing{}, unsupported("the Durable Streams protocol does not support stream listing; use the Pico protocol")
}

func (c *DurableStreamsClient) Close(ctx context.Context, name string) (next string, err error) {
	err = c.core.run(ctx, func() error {
		h := make(http.Header)
		h.Set("Stream-Closed", "true")
		response, e := c.core.send(ctx, wireRequest{method: http.MethodPost, url: c.core.streamURL(name, nil), headers: h})
		if e != nil {
			return e
		}
		_, e = expectDS(response, http.StatusOK, http.StatusNoContent)
		next = response.Header.Get("Stream-Next-Offset")
		return e
	})
	return
}
func (c *DurableStreamsClient) Delete(ctx context.Context, name string) (deleted bool, err error) {
	err = c.core.run(ctx, func() error {
		response, e := c.core.send(ctx, wireRequest{method: http.MethodDelete, url: c.core.streamURL(name, nil)})
		if e != nil {
			return e
		}
		if response.StatusCode == http.StatusNotFound {
			response.Body.Close()
			return nil
		}
		_, e = expectDS(response, http.StatusNoContent)
		deleted = e == nil
		return e
	})
	return
}
