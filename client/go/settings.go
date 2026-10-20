package picomq

import (
	"net/http"
	"strconv"
	"time"
)

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
