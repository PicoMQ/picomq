package picomq

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"
)

type configBody struct {
	Schema         *string `json:"schema"`
	SchemaValidate bool    `json:"schemaValidate"`
	KafkaTopic     *string `json:"kafkaTopic"`
	Partitions     uint32  `json:"partitions"`
	RetentionMs    *uint64 `json:"retentionMs"`
	RetentionBytes *uint64 `json:"retentionBytes"`
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
		headers.Set(prefix+"-Retention-Ms", strconv.FormatInt(durationMillisCeil(o.Retention), 10))
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

func readSettings(header http.Header, prefix string, info *StreamInfo) {
	info.Retention = time.Duration(headerUint(header, prefix+"-Retention-Ms")) * time.Millisecond
	info.RetentionBytes = headerUint(header, prefix+"-Retention-Bytes")
	info.Schema = header.Get(prefix + "-Schema")
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
		var millis any
		if *u.Retention > 0 {
			millis = durationMillisCeil(*u.Retention)
		}
		body["retentionMs"] = millis
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

func durationSecondsCeil(value time.Duration) int64 {
	return int64((value-1)/time.Second) + 1
}

func durationMillisCeil(value time.Duration) int64 {
	return int64((value-1)/time.Millisecond) + 1
}
