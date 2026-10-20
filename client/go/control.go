package picomq

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// TopicConfig is a topic's settings as /_streams reports them. Retention and
// RetentionBytes are zero when unlimited.
type TopicConfig struct {
	Schema         string
	SchemaValidate bool
	KafkaTopic     string
	Partitions     uint32
	Retention      time.Duration
	RetentionBytes uint64
}

// ConfigUpdate changes a topic's settings. A nil field is left alone and a
// pointer to the zero value removes the setting. Partitions grows the topic
// when non-zero and can never shrink it.
type ConfigUpdate struct {
	Schema         *string
	SchemaValidate *bool
	Retention      *time.Duration
	RetentionBytes *uint64
	Partitions     uint32
}

// Schema is a registered schema and its format's content type.
type Schema struct {
	ContentType string
	Body        []byte
}

type configBody struct {
	Schema         *string `json:"schema"`
	SchemaValidate bool    `json:"schemaValidate"`
	KafkaTopic     *string `json:"kafkaTopic"`
	Partitions     uint32  `json:"partitions"`
	RetentionMs    *uint64 `json:"retentionMs"`
	RetentionBytes *uint64 `json:"retentionBytes"`
}

func (c *PicoClient) Config(ctx context.Context, name string) (*TopicConfig, error) {
	return c.core.config(ctx, name)
}
func (c *DurableStreamsClient) Config(ctx context.Context, name string) (*TopicConfig, error) {
	return c.core.config(ctx, name)
}
func (c *PicoClient) UpdateConfig(ctx context.Context, name string, update ConfigUpdate) (TopicConfig, error) {
	return c.core.updateConfig(ctx, name, update)
}
func (c *DurableStreamsClient) UpdateConfig(ctx context.Context, name string, update ConfigUpdate) (TopicConfig, error) {
	return c.core.updateConfig(ctx, name, update)
}
func (c *PicoClient) PutSchema(ctx context.Context, name, contentType string, body []byte) error {
	return c.core.putSchema(ctx, name, contentType, body)
}
func (c *DurableStreamsClient) PutSchema(ctx context.Context, name, contentType string, body []byte) error {
	return c.core.putSchema(ctx, name, contentType, body)
}
func (c *PicoClient) GetSchema(ctx context.Context, name string) (*Schema, error) {
	return c.core.getSchema(ctx, name)
}
func (c *DurableStreamsClient) GetSchema(ctx context.Context, name string) (*Schema, error) {
	return c.core.getSchema(ctx, name)
}
func (c *PicoClient) DeleteSchema(ctx context.Context, name string) (bool, error) {
	return c.core.deleteSchema(ctx, name)
}
func (c *DurableStreamsClient) DeleteSchema(ctx context.Context, name string) (bool, error) {
	return c.core.deleteSchema(ctx, name)
}

func (c *coreClient) config(ctx context.Context, name string) (config *TopicConfig, err error) {
	err = c.run(ctx, func() error {
		response, callErr := c.send(ctx, wireRequest{method: http.MethodGet, url: c.controlURL("/_streams", name)})
		if callErr != nil {
			return callErr
		}
		if response.StatusCode == http.StatusNotFound {
			response.Body.Close()
			config = nil
			return nil
		}
		data, callErr := expectControl(response, http.StatusOK)
		if callErr != nil {
			return callErr
		}
		decoded, callErr := decodeConfig(data)
		config = &decoded
		return callErr
	})
	return
}

func (c *coreClient) updateConfig(ctx context.Context, name string, update ConfigUpdate) (TopicConfig, error) {
	payload, err := json.Marshal(update.body())
	if err != nil {
		return TopicConfig{}, err
	}
	response, err := c.send(ctx, wireRequest{method: http.MethodPatch, url: c.controlURL("/_streams", name), headers: http.Header{"Content-Type": {"application/json"}}, body: payload})
	if err != nil {
		return TopicConfig{}, err
	}
	data, err := expectControl(response, http.StatusOK)
	if err != nil {
		return TopicConfig{}, err
	}
	return decodeConfig(data)
}

// putSchema registers a schema. An empty content type lets the server take
// the format from the name's extension.
func (c *coreClient) putSchema(ctx context.Context, name, contentType string, body []byte) error {
	headers := make(http.Header)
	if contentType != "" {
		headers.Set("Content-Type", contentType)
	}
	response, err := c.send(ctx, wireRequest{method: http.MethodPut, url: c.controlURL("/_schemas", name), headers: headers, body: body})
	if err != nil {
		return err
	}
	_, err = expectControl(response, http.StatusNoContent)
	return err
}

func (c *coreClient) getSchema(ctx context.Context, name string) (schema *Schema, err error) {
	err = c.run(ctx, func() error {
		response, callErr := c.send(ctx, wireRequest{method: http.MethodGet, url: c.controlURL("/_schemas", name)})
		if callErr != nil {
			return callErr
		}
		if response.StatusCode == http.StatusNotFound {
			response.Body.Close()
			schema = nil
			return nil
		}
		contentType := response.Header.Get("Content-Type")
		data, callErr := expectControl(response, http.StatusOK)
		if callErr != nil {
			return callErr
		}
		schema = &Schema{ContentType: contentType, Body: data}
		return nil
	})
	return
}

func (c *coreClient) deleteSchema(ctx context.Context, name string) (bool, error) {
	response, err := c.send(ctx, wireRequest{method: http.MethodDelete, url: c.controlURL("/_schemas", name)})
	if err != nil {
		return false, err
	}
	if _, err := expectControl(response, http.StatusNoContent, http.StatusNotFound); err != nil {
		return false, err
	}
	return response.StatusCode == http.StatusNoContent, nil
}

func (c *coreClient) controlURL(root, name string) string {
	return c.streamURL(root+"/"+strings.TrimLeft(name, "/"), nil)
}

func (o CreateOptions) headers(prefix string) http.Header {
	headers := make(http.Header)
	headers.Set("Content-Type", defaultString(o.ContentType, "application/octet-stream"))
	if o.Partitions > 0 {
		headers.Set(prefix+"-Partitions", strconv.FormatUint(uint64(o.Partitions), 10))
	}
	if o.TTL > 0 {
		headers.Set(prefix+"-TTL", strconv.FormatInt(durationSecondsCeil(o.TTL), 10))
	}
	if o.ExpiresAt != "" {
		headers.Set(prefix+"-Expires-At", o.ExpiresAt)
	}
	if o.Retention > 0 {
		headers.Set(prefix+"-Retention-Ms", strconv.FormatInt(o.Retention.Milliseconds(), 10))
	}
	if o.RetentionBytes > 0 {
		headers.Set(prefix+"-Retention-Bytes", strconv.FormatUint(o.RetentionBytes, 10))
	}
	if o.Schema != "" {
		headers.Set(prefix+"-Schema", o.Schema)
	}
	if o.SchemaValidate {
		headers.Set(prefix+"-Schema-Validate", "true")
	}
	if o.Closed {
		headers.Set(prefix+"-Closed", "true")
	}
	return headers
}

func retentionHeaders(header http.Header, prefix string) (time.Duration, uint64) {
	return time.Duration(headerUint(header, prefix+"-Retention-Ms")) * time.Millisecond, headerUint(header, prefix+"-Retention-Bytes")
}

func (u ConfigUpdate) body() map[string]any {
	body := map[string]any{}
	if u.Schema != nil {
		body["schema"] = nullIfZero(*u.Schema, "")
	}
	if u.SchemaValidate != nil {
		body["schemaValidate"] = *u.SchemaValidate
	}
	if u.Retention != nil {
		body["retentionMs"] = nullIfZero(u.Retention.Milliseconds(), 0)
	}
	if u.RetentionBytes != nil {
		body["retentionBytes"] = nullIfZero(*u.RetentionBytes, 0)
	}
	if u.Partitions > 0 {
		body["partitions"] = u.Partitions
	}
	return body
}

func nullIfZero[T comparable](value, zero T) any {
	if value == zero {
		return nil
	}
	return value
}

func decodeConfig(data []byte) (TopicConfig, error) {
	var body configBody
	if err := json.Unmarshal(data, &body); err != nil {
		return TopicConfig{}, invalidResponse(err)
	}
	config := TopicConfig{SchemaValidate: body.SchemaValidate, Partitions: partitionCount(uint64(body.Partitions))}
	if body.Schema != nil {
		config.Schema = *body.Schema
	}
	if body.KafkaTopic != nil {
		config.KafkaTopic = *body.KafkaTopic
	}
	if body.RetentionMs != nil {
		config.Retention = time.Duration(*body.RetentionMs) * time.Millisecond
	}
	if body.RetentionBytes != nil {
		config.RetentionBytes = *body.RetentionBytes
	}
	return config, nil
}

// expectControl decodes the control routes' errors, {"error": "<message>"}
// on either protocol.
func expectControl(response *http.Response, expected ...int) ([]byte, error) {
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
		Error string `json:"error"`
	}{}
	_ = json.Unmarshal(data, &parsed)
	message := defaultString(parsed.Error, strings.TrimSpace(string(data)))
	kind := ErrorOther
	switch response.StatusCode {
	case 400:
		kind = ErrorBadRequest
	case 401:
		kind = ErrorUnauthenticated
	case 403:
		kind = ErrorPermission
	case 404:
		kind = ErrorNotFound
	case 409:
		kind = ErrorConflict
	}
	code := string(kind)
	if kind == ErrorOther {
		code = "http_" + strconv.Itoa(response.StatusCode)
	}
	return nil, &ClientError{Kind: kind, Status: response.StatusCode, Code: code, Message: message}
}
