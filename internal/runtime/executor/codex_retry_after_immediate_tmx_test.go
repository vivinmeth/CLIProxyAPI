package executor

import (
	"errors"
	"net/http"
	"testing"
	"time"
)

// tmx: a transient Codex 429 with Retry-After 0 or a past date means "retry now"; our header
// precedence must keep that instead of falling back to the default cooldown.
func TestCodexHTTPRetryAfterKeepsImmediateRetry(t *testing.T) {
	now := time.Now()
	body := []byte(`{"error":{"type":"rate_limit_error","code":"turn_rate_limit_exceeded","message":"Too many turns"}}`)
	for _, value := range []string{"0", now.Add(-time.Minute).UTC().Format(http.TimeFormat)} {
		got := codexHTTPRetryAfter(http.StatusTooManyRequests, http.Header{"Retry-After": {value}}, body, nil, now)
		if got == nil || *got != 0 {
			t.Fatalf("Retry-After %q: got %v, want an immediate (zero) hint", value, got)
		}
	}
	err := newCodexHTTPStatusErr(http.StatusTooManyRequests, http.Header{"Retry-After": {"0"}}, body, false)
	var hinted interface{ RetryAfter() *time.Duration }
	if !errors.As(err, &hinted) || hinted.RetryAfter() == nil || *hinted.RetryAfter() != 0 {
		t.Fatalf("HTTP refusal lost the immediate retry: %v", err)
	}
	if got := codexHTTPRetryAfter(http.StatusTooManyRequests, http.Header{"Retry-After": {"30"}}, body, nil, now); got == nil || *got != 30*time.Second {
		t.Fatalf("positive Retry-After: got %v, want 30s", got)
	}
}
