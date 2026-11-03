package executor

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v8/sdk/translator"
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

// A payload override that rewrites the model to Claude after every earlier check is still refused
// for a Responses (Codex) request, and nothing reaches the upstream (pr-reviewer r1 S1).
func TestOpenAICompatPayloadOverrideToClaudeRefused(t *testing.T) {
	hits := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"x","choices":[{"message":{"role":"assistant","content":"hi"}}]}`))
	}))
	defer server.Close()
	cfg := &config.Config{
		OpenAICompatibility: []config.OpenAICompatibility{{Name: "test-compat",
			Models: []config.OpenAICompatibilityModel{{Name: "gpt-6-luna", Alias: "luna"}}}},
	}
	cfg.Payload.Override = []config.PayloadRule{{Models: []config.PayloadModelRule{{Name: "*"}},
		Params: map[string]any{"model": "anthropic/claude-sonnet-5"}}}
	exec := NewOpenAICompatExecutor("openai-compatibility", cfg)
	auth := &cliproxyauth.Auth{Provider: "openai-compatibility", Attributes: map[string]string{
		"base_url": server.URL + "/v1", "api_key": "test-key", "compat_name": "test-compat", "provider_key": "test-compat"}}
	req := cliproxyexecutor.Request{Model: "gpt-6-luna", Payload: []byte(`{"model":"luna","input":"hi"}`)}
	opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("openai-response")}
	check := func(name string, err error) {
		t.Helper()
		sc, ok := err.(interface{ StatusCode() int })
		if err == nil || !ok || sc.StatusCode() != http.StatusBadRequest || !strings.Contains(err.Error(), "claude_models_not_for_codex") {
			t.Fatalf("%s: err = %v", name, err)
		}
	}
	_, err := exec.Execute(context.Background(), auth, req, opts)
	check("execute", err)
	opts.Stream = true
	_, err = exec.ExecuteStream(context.Background(), auth, req, opts)
	check("stream", err)
	if hits != 0 {
		t.Fatalf("upstream got %d requests", hits)
	}
	// Without the override the same request goes out.
	cfg.Payload.Override = nil
	opts.Stream = false
	if _, err = exec.Execute(context.Background(), auth, req, opts); err != nil || hits != 1 {
		t.Fatalf("plain request: err %v hits %d", err, hits)
	}
}
