package capture

import (
	"context"
	"sync"
)

// SessionHintGinKey stores a request's holder on Gin, accessed only by its handler goroutine.
const SessionHintGinKey = "tmx.capture.session-hint"

type sessionHintContextKey struct{}

// SessionHint bridges executor metadata back to capture without retaining a pooled
// HTTP context. Conflicting observations deliberately leave the exchange unattributed.
type SessionHint struct {
	mu       sync.Mutex
	id       string
	conflict bool
	closed   bool
}

// Note accepts matching nonempty observations until capture finalizes the request.
func (h *SessionHint) Note(id string) {
	if h == nil || id == "" {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return
	}
	if h.id == "" {
		h.id = id
	} else if h.id != id {
		h.conflict = true
	}
}

// Take closes the holder and returns its sole identity once. A conflict returns empty.
func (h *SessionHint) Take() string {
	if h == nil {
		return ""
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return ""
	}
	h.closed = true
	id := h.id
	h.id = ""
	if h.conflict {
		return ""
	}
	return id
}

// Close discards any identity and prevents late executor writes.
func (h *SessionHint) Close() {
	if h != nil {
		h.Take()
	}
}

// WithSessionHint attaches a request-local holder to an execution context.
func WithSessionHint(ctx context.Context, h *SessionHint) context.Context {
	return context.WithValue(ctx, sessionHintContextKey{}, h)
}

// SessionHintFrom returns the holder without consulting HTTP or Gin state.
func SessionHintFrom(ctx context.Context) *SessionHint {
	if ctx == nil {
		return nil
	}
	h, _ := ctx.Value(sessionHintContextKey{}).(*SessionHint)
	return h
}
