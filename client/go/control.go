package picomq

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
)

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
func (c *coreClient) config(ctx context.Context, name string) (config *TopicConfig, err error) {
	err = c.run(ctx, func() error {
		response, callErr := c.control(ctx, http.MethodGet, "/_streams", name, nil, nil)
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
	response, err := c.control(ctx, http.MethodPatch, "/_streams", name, http.Header{"Content-Type": {"application/json"}}, payload)
	if err != nil {
		return TopicConfig{}, err
	}
	data, err := expectControl(response, http.StatusOK)
	if err != nil {
		return TopicConfig{}, err
	}
	return decodeConfig(data)
}

func (c *coreClient) control(ctx context.Context, method, root, name string, headers http.Header, body []byte) (*http.Response, error) {
	return c.send(ctx, wireRequest{method: method, url: c.pathURL(root+namePath(name), nil), headers: headers, body: body})
}

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
