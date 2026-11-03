package executor

import (
	"net/http"
	"strings"
	"testing"
	"time"
)

// tmxSkipCodexToClaude skips upstream test cases that send a Codex (Responses API) request to a
// Claude provider. The tokenmaxx fork refuses those by owner rule: no Claude model reaches a Codex
// client (internal/tmx/clients, sdk/api/handlers/handlers_routing_tmx.go).
func tmxSkipCodexToClaude(t *testing.T, provider string) {
	t.Helper()
	if provider == "" || strings.HasPrefix(strings.ToLower(provider), "claude") {
		t.Skip("tokenmaxx: Claude models are not served to Codex / Responses API clients")
	}
}

// An OpenCode Go usage limit is the whole account's: the credential cools down for every model, so
// the pool's next account serves them (internal/tmx/opencode.IsAccountLimit).
func TestOpenCodeGoLimitIsCredentialScoped(t *testing.T) {
	body := []byte(`{"type":"error","error":{"type":"GoUsageLimitError","message":"Go usage limit exceeded"},"metadata":{"limitName":"5 hour"}}`)
	h := http.Header{"Retry-After": []string{"17415"}}
	err := newOpenAICompatStatusError(http.StatusTooManyRequests, h, body)
	if !err.IsCredentialScoped() || err.StatusCode() != 429 || err.RetryAfter() == nil || *err.RetryAfter() != 17415*time.Second {
		t.Fatalf("err = %+v", err)
	}
	if other := newOpenAICompatStatusError(http.StatusTooManyRequests, nil, []byte(`{"error":{"message":"slow down"}}`)); other.IsCredentialScoped() {
		t.Fatal("an ordinary 429 became credential-scoped")
	}
}
