package picomq

import (
	"net/url"
	"sync"
)

// routes remembers the owner each ownership redirect pointed at, so later
// requests for the same stream partition go straight to that node.
type routes struct {
	mu     sync.Mutex
	owners map[string]map[string]*url.URL
}

func newRoutes() *routes {
	return &routes{owners: map[string]map[string]*url.URL{}}
}

func (r *routes) resolve(u *url.URL) *url.URL {
	r.mu.Lock()
	owner, ok := r.owners[u.Path][u.Query().Get("partition")]
	r.mu.Unlock()
	if !ok {
		return u
	}

	routed := *u
	routed.Scheme = owner.Scheme
	routed.Host = owner.Host
	return &routed
}

func (r *routes) learn(requested, location *url.URL) {
	owner := &url.URL{Scheme: location.Scheme, Host: location.Host}

	r.mu.Lock()
	defer r.mu.Unlock()
	byPartition, ok := r.owners[requested.Path]
	if !ok {
		byPartition = map[string]*url.URL{}
		r.owners[requested.Path] = byPartition
	}
	byPartition[requested.Query().Get("partition")] = owner
}

func (r *routes) forget(path string) {
	r.mu.Lock()
	delete(r.owners, path)
	r.mu.Unlock()
}
