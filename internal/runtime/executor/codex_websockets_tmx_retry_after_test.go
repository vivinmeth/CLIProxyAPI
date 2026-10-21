package executor

import (
	"net/http"
	"testing"
	"time"

	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

// tmx: a terminal error on an established Codex WebSocket keeps its header-derived
// Retry-After, like an HTTP refusal.
func TestParseCodexWebsocketErrorUsesHeaderRetryAfter(t *testing.T) {
	retryAfterOf := func(t *testing.T, err error) time.Duration {
		t.Helper()
		retryable, ok := err.(interface{ RetryAfter() *time.Duration })
		if !ok || retryable.RetryAfter() == nil {
			t.Fatalf("no RetryAfter on %#v", err)
		}
		return *retryable.RetryAfter()
	}
	t.Run("turn throttle", func(t *testing.T) {
		err, ok := parseCodexWebsocketError([]byte(`{"type":"error","status":429,"error":{"type":"rate_limit_error","code":"turn_rate_limit_exceeded","message":"Too many turns"},"headers":{"Retry-After":"120","Set-Cookie":"session=secret","X-Codex-Primary-Used-Percent":"40"}}`))
		if !ok {
			t.Fatal("not parsed")
		}
		if got := retryAfterOf(t, err); got != 120*time.Second {
			t.Fatalf("RetryAfter = %v, want 120s", got)
		}
		if got := cliproxyauth.SafeResponseHeaders(err).Get("Retry-After"); got != "120" {
			t.Fatalf("safe Retry-After = %q, want 120", got)
		}
		headers := err.(interface{ Headers() http.Header }).Headers()
		if headers.Get("Set-Cookie") != "" || headers.Get("X-Codex-Primary-Used-Percent") != "40" {
			t.Fatalf("retained headers must be the retry/quota allowlist: %v", headers)
		}
	})
	t.Run("usage limit takes the later deadline", func(t *testing.T) {
		err, ok := parseCodexWebsocketError([]byte(`{"type":"error","status":429,"body":{"error":{"type":"usage_limit_reached","message":"usage limit reached","resets_in_seconds":7}},"headers":{"retry-after":"3600"}}`))
		if !ok {
			t.Fatal("not parsed")
		}
		if got := retryAfterOf(t, err); got < 3599*time.Second || got > 3600*time.Second {
			t.Fatalf("RetryAfter = %v, want the later 3600s", got)
		}
	})
	t.Run("connection limit stays immediate", func(t *testing.T) {
		err, ok := parseCodexWebsocketError([]byte(`{"type":"error","status":429,"error":{"code":"websocket_connection_limit_reached"},"headers":{"retry-after":"30"}}`))
		if !ok {
			t.Fatal("not parsed")
		}
		if got := retryAfterOf(t, err); got != 0 {
			t.Fatalf("RetryAfter = %v, want 0", got)
		}
	})
}
