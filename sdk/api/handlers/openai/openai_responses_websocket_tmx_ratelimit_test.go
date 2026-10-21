package openai

import (
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/interfaces"
	"github.com/tidwall/gjson"
)

// tmx: Codex's turn rate limit must reach the client with Retry-After so Codex waits instead
// of reconnecting (each reconnect is a new turn on the throttled account).
func TestResponsesWebsocketExposesCodexTurnRateLimit(t *testing.T) {
	body := `{"status":429,"error":{"type":"rate_limit_error","code":"turn_rate_limit_exceeded","message":"Codex is at capacity, please try again later"}}`
	errMsg := &interfaces.ErrorMessage{
		StatusCode: http.StatusTooManyRequests,
		Error:      errors.New(body),
		Addon:      http.Header{"Retry-After": {"293"}},
	}
	if !shouldExposeResponsesUpstreamError(errMsg) {
		t.Fatal("turn_rate_limit_exceeded must be exposed")
	}
	payload, err := buildResponsesWebsocketErrorPayload(errMsg)
	if err != nil {
		t.Fatal(err)
	}
	if got := gjson.GetBytes(payload, "status").Int(); got != 429 {
		t.Errorf("status = %d, want 429: %s", got, payload)
	}
	if got := gjson.GetBytes(payload, "headers.Retry-After").String(); got != "293" {
		t.Errorf("Retry-After = %q, want 293: %s", got, payload)
	}
	if got := gjson.GetBytes(payload, "error.code").String(); got != "turn_rate_limit_exceeded" {
		t.Errorf("error.code = %q: %s", got, payload)
	}

	other := &interfaces.ErrorMessage{
		StatusCode: http.StatusTooManyRequests,
		Error:      errors.New(`{"error":{"type":"usage_limit_reached","message":"limit"}}`),
	}
	if shouldExposeResponsesUpstreamError(other) {
		t.Fatal("other 429s keep upstream's silent close")
	}
}

type wsRetryAfterTestError struct{ retryAfter time.Duration }

func (e wsRetryAfterTestError) Error() string {
	return `{"error":{"type":"usage_limit_reached","message":"limit"}}`
}
func (e wsRetryAfterTestError) StatusCode() int            { return http.StatusTooManyRequests }
func (e wsRetryAfterTestError) RetryAfter() *time.Duration { return &e.retryAfter }

// tmx: the envelope's Retry-After is the computed deadline, not the raw upstream header.
func TestResponsesWebsocketErrorUsesComputedRetryAfter(t *testing.T) {
	payload, err := buildResponsesWebsocketErrorPayload(&interfaces.ErrorMessage{
		StatusCode: http.StatusTooManyRequests,
		Error:      wsRetryAfterTestError{retryAfter: time.Hour},
		Addon:      http.Header{"Retry-After": {"60"}, "X-Codex-Primary-Used-Percent": {"100"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := gjson.GetBytes(payload, "headers.Retry-After").String(); got != "3600" {
		t.Errorf("Retry-After = %q, want 3600: %s", got, payload)
	}
	if got := gjson.GetBytes(payload, "headers.X-Codex-Primary-Used-Percent").String(); got != "100" {
		t.Errorf("other headers must stay: %s", payload)
	}
}
