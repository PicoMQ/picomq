package picomq

import (
	"net/url"
	"strings"
	"sync"
)

// routeKey names a stream partition by its path relative to the client's
// endpoint, so it stays the same whichever node a request went to.
type routeKey struct {
	path      string
	partition string
}

// routes remembers the owner each ownership redirect pointed at, so later
// requests for the same stream partition go straight to that node. An owner
// is kept as the base URL of its redirect, which may carry a path prefix when
// nodes sit behind a gateway.
type routes struct {
	prefix string
	mu     sync.Mutex
	owners map[string]map[string]*url.URL
}

func newRoutes(endpoint *url.URL) *routes {
	return &routes{
		prefix: strings.TrimRight(endpoint.EscapedPath(), "/"),
		owners: map[string]map[string]*url.URL{},
	}
}

// key is false for a URL outside the client's endpoint.
func (r *routes) key(u *url.URL) (routeKey, bool) {
	path, ok := strings.CutPrefix(u.EscapedPath(), r.prefix)
	if !ok {
		return routeKey{}, false
	}

	return routeKey{path: path, partition: u.Query().Get("partition")}, true
}

func (r *routes) resolve(key routeKey, u *url.URL) *url.URL {
	r.mu.Lock()
	owner, ok := r.owners[key.path][key.partition]
	r.mu.Unlock()
	if !ok {
		return u
	}

	escaped := strings.TrimRight(owner.EscapedPath(), "/") + key.path
	path, err := url.PathUnescape(escaped)
	if err != nil {
		return u
	}

	routed := *u
	routed.Scheme = owner.Scheme
	routed.Host = owner.Host
	routed.Path = path
	routed.RawPath = escaped
	return &routed
}

// learn skips a Location that does not end in the stream path, since it has
// no recognizable base.
func (r *routes) learn(key routeKey, location *url.URL) {
	base, ok := strings.CutSuffix(location.EscapedPath(), key.path)
	if !ok {
		return
	}
	path, err := url.PathUnescape(base)
	if err != nil {
		return
	}
	owner := &url.URL{Scheme: location.Scheme, Host: location.Host, Path: path, RawPath: base}

	r.mu.Lock()
	defer r.mu.Unlock()
	byPartition, ok := r.owners[key.path]
	if !ok {
		byPartition = map[string]*url.URL{}
		r.owners[key.path] = byPartition
	}
	byPartition[key.partition] = owner
}

// forget drops every partition of the key's stream.
func (r *routes) forget(key routeKey) {
	r.mu.Lock()
	delete(r.owners, key.path)
	r.mu.Unlock()
}
