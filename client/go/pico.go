package picomq

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

func (c *PicoClient) Stream(name string) *PicoStream { return &PicoStream{client: c, name: name} }

func (c *PicoClient) Create(ctx context.Context, name, contentType string, ttl time.Duration) (created bool, err error) {
	err = c.core.run(ctx, func() error {
		headers := make(http.Header)
		headers.Set("Content-Type", contentType)
		if ttl > 0 {
			headers.Set("Pico-TTL", strconv.FormatInt(durationSecondsCeil(ttl), 10))
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

func (c *PicoClient) JoinGroup(ctx context.Context, group string, subscription []string, options JoinOptions) (GroupMembership, error) {
	request := struct {
		Subscription       []string `json:"subscription"`
		MemberID           string   `json:"memberId,omitempty"`
		InstanceID         string   `json:"instanceId,omitempty"`
		ClientID           string   `json:"clientId,omitempty"`
		SessionTimeoutMs   int64    `json:"sessionTimeoutMs,omitempty"`
		RebalanceTimeoutMs int64    `json:"rebalanceTimeoutMs,omitempty"`
	}{
		Subscription:       subscription,
		MemberID:           options.MemberID,
		InstanceID:         options.InstanceID,
		ClientID:           options.ClientID,
		SessionTimeoutMs:   options.SessionTimeout.Milliseconds(),
		RebalanceTimeoutMs: options.RebalanceTimeout.Milliseconds(),
	}
	if request.Subscription == nil {
		request.Subscription = []string{}
	}

	data, err := c.groupCall(ctx, http.MethodPost, c.core.groupURL(nil, group, "members"), request, http.StatusOK)
	if err != nil {
		return GroupMembership{}, err
	}

	var raw struct {
		MemberID   *string  `json:"memberId"`
		Generation *int32   `json:"generation"`
		Assignment []string `json:"assignment"`
		Members    []string `json:"members"`
	}
	if err = json.Unmarshal(data, &raw); err != nil {
		return GroupMembership{}, invalidResponse(err)
	}
	if raw.MemberID == nil || raw.Generation == nil {
		return GroupMembership{}, &ClientError{Kind: ErrorInvalidResponse, Code: "invalid_response", Message: "join response lacks memberId or generation"}
	}

	return GroupMembership{
		MemberID:   *raw.MemberID,
		Generation: *raw.Generation,
		Assignment: nonNil(raw.Assignment),
		Members:    nonNil(raw.Members),
	}, nil
}

func (c *PicoClient) GroupAssignment(ctx context.Context, group string, fence MemberFence) (GroupAssignment, error) {
	query := url.Values{"generation": {strconv.FormatInt(int64(fence.Generation), 10)}}
	if fence.InstanceID != "" {
		query.Set("instanceId", fence.InstanceID)
	}

	data, err := c.groupCall(ctx, http.MethodGet, c.core.groupURL(query, group, "members", fence.MemberID), nil, http.StatusOK)
	if err != nil {
		return GroupAssignment{}, err
	}

	var raw struct {
		Generation *int32   `json:"generation"`
		Assignment []string `json:"assignment"`
	}
	if err = json.Unmarshal(data, &raw); err != nil {
		return GroupAssignment{}, invalidResponse(err)
	}
	if raw.Generation == nil {
		return GroupAssignment{}, &ClientError{Kind: ErrorInvalidResponse, Code: "invalid_response", Message: "assignment response lacks generation"}
	}

	return GroupAssignment{Generation: *raw.Generation, Assignment: nonNil(raw.Assignment)}, nil
}

func (c *PicoClient) Heartbeat(ctx context.Context, group string, fence MemberFence) error {
	request := struct {
		Generation int32  `json:"generation"`
		InstanceID string `json:"instanceId,omitempty"`
	}{Generation: fence.Generation, InstanceID: fence.InstanceID}

	_, err := c.groupCall(ctx, http.MethodPost, c.core.groupURL(nil, group, "members", fence.MemberID, "heartbeat"), request, http.StatusNoContent)
	return err
}

func (c *PicoClient) LeaveGroup(ctx context.Context, group, memberID, instanceID string) error {
	var query url.Values
	if instanceID != "" {
		query = url.Values{"instanceId": {instanceID}}
	}

	_, err := c.groupCall(ctx, http.MethodDelete, c.core.groupURL(query, group, "members", memberID), nil, http.StatusNoContent)
	return err
}

func (c *PicoClient) CommitOffsets(ctx context.Context, group string, offsets Offsets, fence *MemberFence) error {
	request := struct {
		Offsets    Offsets `json:"offsets"`
		MemberID   string  `json:"memberId,omitempty"`
		Generation *int32  `json:"generation,omitempty"`
		InstanceID string  `json:"instanceId,omitempty"`
	}{Offsets: offsets}
	if request.Offsets == nil {
		request.Offsets = Offsets{}
	}
	if fence != nil {
		generation := fence.Generation
		request.MemberID, request.Generation, request.InstanceID = fence.MemberID, &generation, fence.InstanceID
	}

	_, err := c.groupCall(ctx, http.MethodPut, c.core.groupURL(nil, group, "offsets"), request, http.StatusNoContent)
	return err
}

func (c *PicoClient) FetchOffsets(ctx context.Context, group string, streams []string) (offsets Offsets, err error) {
	err = c.core.run(ctx, func() error {
		var query url.Values
		if len(streams) > 0 {
			query = url.Values{"stream": streams}
		}

		data, callErr := c.groupCall(ctx, http.MethodGet, c.core.groupURL(query, group, "offsets"), nil, http.StatusOK)
		if callErr != nil {
			return callErr
		}

		var raw struct {
			Offsets Offsets `json:"offsets"`
		}
		if callErr = json.Unmarshal(data, &raw); callErr != nil {
			return invalidResponse(callErr)
		}

		offsets = raw.Offsets
		if offsets == nil {
			offsets = Offsets{}
		}
		return nil
	})
	return
}

func (c *PicoClient) DescribeGroup(ctx context.Context, group string) (description GroupDescription, err error) {
	err = c.core.run(ctx, func() error {
		data, callErr := c.groupCall(ctx, http.MethodGet, c.core.groupURL(nil, group), nil, http.StatusOK)
		if callErr != nil {
			return callErr
		}

		var raw struct {
			Group        *string `json:"group"`
			State        *string `json:"state"`
			Generation   int32   `json:"generation"`
			ProtocolType string  `json:"protocolType"`
			Members      []struct {
				MemberID     string   `json:"memberId"`
				InstanceID   string   `json:"instanceId"`
				ClientID     string   `json:"clientId"`
				Subscription []string `json:"subscription"`
				Assignment   []string `json:"assignment"`
			} `json:"members"`
		}
		if callErr = json.Unmarshal(data, &raw); callErr != nil {
			return invalidResponse(callErr)
		}
		if raw.Group == nil || raw.State == nil {
			return &ClientError{Kind: ErrorInvalidResponse, Code: "invalid_response", Message: "describe response lacks group or state"}
		}

		description = GroupDescription{
			Group:        *raw.Group,
			State:        *raw.State,
			Generation:   raw.Generation,
			ProtocolType: raw.ProtocolType,
			Members:      make([]MemberDescription, 0, len(raw.Members)),
		}
		for _, member := range raw.Members {
			description.Members = append(description.Members, MemberDescription{
				MemberID:     member.MemberID,
				InstanceID:   member.InstanceID,
				ClientID:     member.ClientID,
				Subscription: member.Subscription,
				Assignment:   member.Assignment,
			})
		}
		return nil
	})
	return
}

func (c *PicoClient) ListGroups(ctx context.Context) (groups []GroupSummary, err error) {
	err = c.core.run(ctx, func() error {
		data, callErr := c.groupCall(ctx, http.MethodGet, c.core.groupURL(nil), nil, http.StatusOK)
		if callErr != nil {
			return callErr
		}

		var raw struct {
			Groups []struct {
				Group string `json:"group"`
				State string `json:"state"`
			} `json:"groups"`
		}
		if callErr = json.Unmarshal(data, &raw); callErr != nil {
			return invalidResponse(callErr)
		}

		groups = make([]GroupSummary, 0, len(raw.Groups))
		for _, group := range raw.Groups {
			groups = append(groups, GroupSummary{Group: group.Group, State: group.State})
		}
		return nil
	})
	return
}

func (c *PicoClient) groupCall(ctx context.Context, method, target string, request any, expected ...int) ([]byte, error) {
	wire := wireRequest{method: method, url: target}
	if request != nil {
		payload, err := json.Marshal(request)
		if err != nil {
			return nil, err
		}
		wire.headers = http.Header{"Content-Type": {"application/json"}}
		wire.body = payload
	}

	response, err := c.core.send(ctx, wire)
	if err != nil {
		return nil, err
	}

	return expectPico(response, expected...)
}

func (c *coreClient) groupURL(query url.Values, segments ...string) string {
	u := *c.baseURL

	path := strings.TrimRight(c.baseURL.Path, "/") + "/_groups"
	raw := strings.TrimRight(c.baseURL.EscapedPath(), "/") + "/_groups"
	for _, segment := range segments {
		path += "/" + segment
		raw += "/" + url.PathEscape(segment)
	}

	u.Path, u.RawPath = path, raw
	u.RawQuery = query.Encode()
	return u.String()
}

func nonNil(values []string) []string {
	if values == nil {
		return []string{}
	}
	return values
}

func defaultString(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
