// Package codexturn keeps Codex's per-turn sticky-routing token (x-codex-turn-state) working
// through the proxy.
//
// The ChatGPT Codex backend returns x-codex-turn-state on the first response of a turn, and the
// client must replay it on every later request of that turn (tool-call follow-ups, retries,
// continuations). Without it each request counts as a new turn and bursts hit
// turn_rate_limit_exceeded. The token belongs to the credential that received it, so it is only
// replayed upstream on that same credential; an unknown token (e.g. after a restart) is dropped,
// which costs at most one extra turn.
package codexturn

import (
	"net/http"
	"strings"
	"sync"
	"time"
)

// Header is the turn-state header name as sent by Codex and the ChatGPT backend.
const Header = "X-Codex-Turn-State"

const (
	ttl        = 6 * time.Hour
	maxEntries = 20000
)

type entry struct {
	authID string
	seen   time.Time
}

var (
	mu      sync.Mutex
	entries = make(map[string]entry)
	nowFunc = time.Now
)

// Remember records the turn-state token from an upstream response as owned by authID.
func Remember(upstream http.Header, authID string) {
	state := strings.TrimSpace(headerValue(upstream))
	authID = strings.TrimSpace(authID)
	if state == "" || authID == "" {
		return
	}
	now := nowFunc()
	mu.Lock()
	defer mu.Unlock()
	if len(entries) >= maxEntries {
		pruneLocked(now)
	}
	entries[state] = entry{authID: authID, seen: now}
}

// Allowed reports whether a client-supplied token may be replayed upstream on authID.
func Allowed(state, authID string) bool {
	state = strings.TrimSpace(state)
	authID = strings.TrimSpace(authID)
	if state == "" || authID == "" {
		return false
	}
	now := nowFunc()
	mu.Lock()
	defer mu.Unlock()
	e, ok := entries[state]
	if !ok || now.Sub(e.seen) > ttl {
		return false
	}
	if e.authID != authID {
		return false
	}
	e.seen = now
	entries[state] = e
	return true
}

// ApplyUpstream sets the client's turn-state token on an upstream request when it was issued to
// authID, and removes it otherwise.
func ApplyUpstream(dst http.Header, client http.Header, authID string) {
	if dst == nil {
		return
	}
	state := strings.TrimSpace(headerValue(client))
	deleteHeader(dst)
	if Allowed(state, authID) {
		dst.Set(Header, state)
	}
}

// ClientHeaders returns the upstream response headers that must reach the client even when
// generic header passthrough is off. Returns nil when there are none.
func ClientHeaders(upstream http.Header) http.Header {
	state := strings.TrimSpace(headerValue(upstream))
	if state == "" {
		return nil
	}
	out := make(http.Header, 1)
	out.Set(Header, state)
	return out
}

func headerValue(h http.Header) string {
	if h == nil {
		return ""
	}
	if v := h.Get(Header); v != "" {
		return v
	}
	for k, vs := range h {
		if strings.EqualFold(k, Header) && len(vs) > 0 {
			return vs[0]
		}
	}
	return ""
}

func deleteHeader(h http.Header) {
	for k := range h {
		if strings.EqualFold(k, Header) {
			delete(h, k)
		}
	}
}

func pruneLocked(now time.Time) {
	for k, e := range entries {
		if now.Sub(e.seen) > ttl {
			delete(entries, k)
		}
	}
	if len(entries) < maxEntries {
		return
	}
	// Still full of live tokens: drop the oldest half.
	cutoff := now
	for _, e := range entries {
		if e.seen.Before(cutoff) {
			cutoff = e.seen
		}
	}
	mid := cutoff.Add(now.Sub(cutoff) / 2)
	for k, e := range entries {
		if e.seen.Before(mid) {
			delete(entries, k)
		}
	}
}

func resetForTest() {
	mu.Lock()
	entries = make(map[string]entry)
	mu.Unlock()
}
