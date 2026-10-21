package executor

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v8/sdk/translator"
)

// firstRefusalRetryAfter sends one request through a manager whose only credential is refused
// by server and returns the Retry-After the client error writer would send.
func firstRefusalRetryAfter(t *testing.T, provider, model string, exec cliproxyauth.ProviderExecutor, server *httptest.Server, format sdktranslator.Format, payload string) int {
	t.Helper()
	manager := cliproxyauth.NewManager(nil, nil, nil)
	manager.SetRetryConfig(0, 0, 0)
	manager.RegisterExecutor(exec)
	auth := &cliproxyauth.Auth{ID: provider + "-first-429-" + strconv.FormatInt(time.Now().UnixNano(), 36), Provider: provider,
		Attributes: map[string]string{"base_url": server.URL, "api_key": "synthetic-test"}}
	reg := registry.GetGlobalRegistry()
	reg.RegisterClient(auth.ID, provider, []*registry.ModelInfo{{ID: model}})
	t.Cleanup(func() { reg.UnregisterClient(auth.ID) })
	if _, err := manager.Register(context.Background(), auth); err != nil {
		t.Fatalf("register auth: %v", err)
	}
	_, err := manager.Execute(context.Background(), []string{provider}, cliproxyexecutor.Request{Model: model, Payload: []byte(payload)},
		cliproxyexecutor.Options{SourceFormat: format})
	if err == nil {
		t.Fatal("expected the refusal to reach the caller")
	}
	raw := cliproxyauth.SafeResponseHeaders(err).Get("Retry-After")
	if raw == "" {
		t.Fatalf("first 429 has no Retry-After: %v", err)
	}
	seconds, errParse := strconv.Atoi(raw)
	if errParse != nil {
		t.Fatalf("Retry-After %q is not delta-seconds", raw)
	}
	return seconds
}

func TestFirstClaude429CarriesRetryAfter(t *testing.T) {
	for _, tc := range []struct {
		name     string
		reset    time.Duration
		min, max int
	}{
		{"5h reset", 2 * time.Hour, 7190, 7240},
		{"weekly reset capped at a day", 72 * time.Hour, 86400, 86400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Anthropic-Ratelimit-Unified-Status", "rejected")
				w.Header().Set("Anthropic-Ratelimit-Unified-5h-Status", "rejected")
				w.Header().Set("Anthropic-Ratelimit-Unified-5h-Reset", strconv.FormatInt(time.Now().Add(tc.reset).Unix(), 10))
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusTooManyRequests)
				_, _ = w.Write([]byte(`{"type":"error","error":{"type":"rate_limit_error","message":"usage limit"}}`))
			}))
			defer server.Close()
			got := firstRefusalRetryAfter(t, "claude", "claude-opus-5", NewClaudeExecutor(&config.Config{}), server, sdktranslator.FormatClaude,
				`{"model":"claude-opus-5","messages":[{"role":"user","content":"hi"}]}`)
			if got < tc.min || got > tc.max {
				t.Fatalf("Retry-After = %d, want %d..%d", got, tc.min, tc.max)
			}
		})
	}
}

func TestFirstCodex429CarriesRetryAfter(t *testing.T) {
	server := codexRefusalServer(t, http.StatusTooManyRequests, http.Header{"Retry-After": []string{"120"}},
		`{"error":{"type":"rate_limit_error","code":"turn_rate_limit_exceeded","message":"Too many turns"}}`)
	got := firstRefusalRetryAfter(t, "codex", "gpt-5.4", NewCodexExecutor(&config.Config{}), server, sdktranslator.FromString("openai-response"),
		`{"model":"gpt-5.4","input":"synthetic"}`)
	if got < 119 || got > 120 {
		t.Fatalf("Retry-After = %d, want 120", got)
	}
}
