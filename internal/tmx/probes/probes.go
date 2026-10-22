// Package probes keeps health-check and discovery probes out of the error request-log slots
// (tokenmaxx fork).
//
// The proxy keeps only the newest error-logs-max-files error logs. LAN tools probe it every few
// minutes (seen on prod: HEAD /api/hello from Bun/1.4.4 on four machines), each probe a 404 that
// took a slot, so real upstream errors (429 bodies) rolled off within an hour. A request that
// matched no route, carried no body and failed 404/405 is a probe: it is counted here, per
// source, instead of written as an error log. Everything else keeps its slot.
package probes

import (
	"net/http"
	"sort"
	"sync"
	"time"
)

const (
	maxSources = 200
	maxField   = 200
)

// Source is one kind of probe (method, path, client, user agent, status) and how often it came.
type Source struct {
	Method    string    `json:"method"`
	Path      string    `json:"path"`
	ClientIP  string    `json:"client_ip"`
	UserAgent string    `json:"user_agent"`
	Status    int       `json:"status"`
	Count     int64     `json:"count"`
	FirstSeen time.Time `json:"first_seen"`
	LastSeen  time.Time `json:"last_seen"`
}

// Summary is what has been counted since the process started.
type Summary struct {
	Since   time.Time `json:"since"`
	Total   int64     `json:"total"`
	Other   int64     `json:"other"` // probes beyond the first 200 distinct sources
	Sources []Source  `json:"sources"`
}

type key struct {
	method, path, ip, ua string
	status               int
}

var (
	mu      sync.Mutex
	since   = time.Now().UTC()
	total   int64
	other   int64
	sources = map[key]*Source{}
)

// Is reports whether a failed request is a probe: no route matched (routePath ""), no request
// body, status 404 or 405, and no upstream error attached.
func Is(routePath string, status int, hasBody, hasUpstreamErrors bool) bool {
	if routePath != "" || hasBody || hasUpstreamErrors {
		return false
	}
	return status == http.StatusNotFound || status == http.StatusMethodNotAllowed
}

// Record counts one probe.
func Record(method, path, clientIP, userAgent string, status int, at time.Time) {
	k := key{clip(method), clip(path), clip(clientIP), clip(userAgent), status}
	at = at.UTC()
	mu.Lock()
	defer mu.Unlock()
	total++
	s := sources[k]
	if s == nil {
		if len(sources) >= maxSources {
			other++
			return
		}
		s = &Source{Method: k.method, Path: k.path, ClientIP: k.ip, UserAgent: k.ua, Status: status, FirstSeen: at}
		sources[k] = s
	}
	s.Count++
	s.LastSeen = at
}

// Snapshot returns the counts, most frequent source first.
func Snapshot() Summary {
	mu.Lock()
	defer mu.Unlock()
	out := Summary{Since: since, Total: total, Other: other, Sources: make([]Source, 0, len(sources))}
	for _, s := range sources {
		out.Sources = append(out.Sources, *s)
	}
	sort.Slice(out.Sources, func(i, j int) bool {
		a, b := out.Sources[i], out.Sources[j]
		if a.Count != b.Count {
			return a.Count > b.Count
		}
		return a.LastSeen.After(b.LastSeen)
	})
	return out
}

// ResetForTest clears the counters.
func ResetForTest() {
	mu.Lock()
	defer mu.Unlock()
	since, total, other, sources = time.Now().UTC(), 0, 0, map[key]*Source{}
}

func clip(s string) string {
	if len(s) > maxField {
		return s[:maxField]
	}
	return s
}
