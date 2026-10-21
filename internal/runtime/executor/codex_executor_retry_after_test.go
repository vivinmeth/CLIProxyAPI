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

// codexRefusalCall runs one Codex entry point against base and returns its error.
type codexRefusalCall struct {
	name string
	call func(base string) error
}

func codexRefusalCalls() []codexRefusalCall {
	req := cliproxyexecutor.Request{Model: "gpt-5.4", Payload: []byte(`{"model":"gpt-5.4","input":"synthetic"}`)}
	format := sdktranslator.FromString("openai-response")
	httpAuth := func(base string) *cliproxyauth.Auth {
		return &cliproxyauth.Auth{Attributes: map[string]string{"base_url": base, "api_key": "synthetic-test"}}
	}
	wsAuth := func(base string) *cliproxyauth.Auth {
		return &cliproxyauth.Auth{ID: "codex-ws-retry-after", Provider: "codex",
			Attributes: map[string]string{"base_url": base, "api_key": "synthetic-test", "websockets": "true"}}
	}
	return []codexRefusalCall{
		{"execute", func(base string) error {
			_, err := NewCodexExecutor(&config.Config{}).Execute(context.Background(), httpAuth(base), req, cliproxyexecutor.Options{SourceFormat: format})
			return err
		}},
		{"stream", func(base string) error {
			_, err := NewCodexExecutor(&config.Config{}).ExecuteStream(context.Background(), httpAuth(base), req, cliproxyexecutor.Options{SourceFormat: format, Stream: true})
			return err
		}},
		{"compact", func(base string) error {
			_, err := NewCodexExecutor(&config.Config{}).Execute(context.Background(), httpAuth(base), req, cliproxyexecutor.Options{SourceFormat: format, Alt: "responses/compact"})
			return err
		}},
		{"websocket-handshake", func(base string) error {
			_, err := NewCodexWebsocketsExecutor(&config.Config{}).Execute(cliproxyexecutor.WithUpstreamAttemptTracker(context.Background()), wsAuth(base), req, cliproxyexecutor.Options{SourceFormat: format, ResponseFormat: format})
			return err
		}},
		{"websocket-handshake-stream", func(base string) error {
			_, err := NewCodexWebsocketsExecutor(&config.Config{}).ExecuteStream(cliproxyexecutor.WithUpstreamAttemptTracker(context.Background()), wsAuth(base), req, cliproxyexecutor.Options{SourceFormat: format, ResponseFormat: format, Stream: true})
			return err
		}},
	}
}

func codexRefusalServer(t *testing.T, status int, headers http.Header, body string) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		for key, values := range headers {
			for _, value := range values {
				w.Header().Add(key, value)
			}
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)
	return server
}

func codexErrRetryAfter(t *testing.T, err error) time.Duration {
	t.Helper()
	if err == nil {
		t.Fatal("expected a refusal")
	}
	rap, ok := err.(interface{ RetryAfter() *time.Duration })
	if !ok || rap.RetryAfter() == nil {
		t.Fatalf("provider deadline lost: %#v", err)
	}
	return *rap.RetryAfter()
}

// The red-team reproduction (multi-account review, finding 2): a transient turn throttle with
// Retry-After must hand that delay to the manager on every HTTP entry point.
func TestCodexHTTPRefusalKeepsRetryAfter(t *testing.T) {
	headers := http.Header{"Retry-After": []string{"120"}, "Set-Cookie": []string{"session=secret"}}
	server := codexRefusalServer(t, http.StatusTooManyRequests, headers,
		`{"error":{"type":"rate_limit_error","code":"turn_rate_limit_exceeded","message":"Too many turns"}}`)
	for _, tc := range codexRefusalCalls() {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.call(server.URL)
			if got := codexErrRetryAfter(t, err); got != 120*time.Second {
				t.Fatalf("RetryAfter = %v, want 120s", got)
			}
			carrier, ok := err.(interface{ Headers() http.Header })
			if !ok {
				t.Fatalf("error must carry the safe response headers: %#v", err)
			}
			if got := carrier.Headers(); got.Get("Retry-After") != "120" || got.Get("Set-Cookie") != "" {
				t.Fatalf("error headers = %v; want Retry-After only", got)
			}
		})
	}
}

func TestCodexHTTPRetryAfterHTTPDate(t *testing.T) {
	deadline := time.Now().Add(90 * time.Second).UTC()
	server := codexRefusalServer(t, http.StatusTooManyRequests, http.Header{"Retry-After": []string{deadline.Format(http.TimeFormat)}},
		`{"error":{"type":"rate_limit_error","code":"turn_rate_limit_exceeded","message":"Too many turns"}}`)
	got := codexErrRetryAfter(t, codexRefusalCalls()[0].call(server.URL))
	if got < 85*time.Second || got > 90*time.Second {
		t.Fatalf("RetryAfter = %v, want about 90s", got)
	}
}

func TestCodexHTTPUsageLimitLaterDeadlineWins(t *testing.T) {
	now := time.Now()
	weekly := now.Add(3 * 24 * time.Hour)
	t.Run("exhausted window reset outranks body reset and Retry-After", func(t *testing.T) {
		headers := http.Header{
			"Retry-After":                      []string{"60"},
			"X-Codex-Primary-Used-Percent":     []string{"40"},
			"X-Codex-Primary-Reset-At":         []string{strconv.FormatInt(now.Add(5*24*time.Hour).Unix(), 10)},
			"X-Codex-Secondary-Used-Percent":   []string{"100"},
			"X-Codex-Secondary-Reset-At":       []string{strconv.FormatInt(weekly.Unix(), 10)},
			"X-Codex-Secondary-Window-Minutes": []string{"10080"},
		}
		server := codexRefusalServer(t, http.StatusTooManyRequests, headers,
			`{"error":{"type":"usage_limit_reached","message":"The usage limit has been reached","resets_in_seconds":3600}}`)
		got := codexErrRetryAfter(t, codexRefusalCalls()[0].call(server.URL))
		// The primary window has room left: its later reset must not count.
		if want := time.Until(weekly); got < want-5*time.Second || got > want+time.Second {
			t.Fatalf("RetryAfter = %v, want the exhausted weekly reset (%v)", got, want)
		}
	})
	t.Run("Retry-After later than the body reset", func(t *testing.T) {
		server := codexRefusalServer(t, http.StatusTooManyRequests, http.Header{"Retry-After": []string{"7200"}},
			`{"error":{"type":"usage_limit_reached","message":"The usage limit has been reached","resets_in_seconds":3600}}`)
		if got := codexErrRetryAfter(t, codexRefusalCalls()[0].call(server.URL)); got < 7195*time.Second || got > 7200*time.Second {
			t.Fatalf("RetryAfter = %v, want 2h", got)
		}
	})
	t.Run("body reset later than Retry-After", func(t *testing.T) {
		server := codexRefusalServer(t, http.StatusTooManyRequests, http.Header{"Retry-After": []string{"60"}},
			`{"error":{"type":"usage_limit_reached","message":"The usage limit has been reached","resets_in_seconds":3600}}`)
		if got := codexErrRetryAfter(t, codexRefusalCalls()[0].call(server.URL)); got != time.Hour {
			t.Fatalf("RetryAfter = %v, want 1h", got)
		}
	})
}

func TestCodexHTTPTransientThrottleIgnoresWindowResets(t *testing.T) {
	headers := http.Header{
		"X-Codex-Primary-Used-Percent": []string{"100"},
		"X-Codex-Primary-Reset-At":     []string{strconv.FormatInt(time.Now().Add(24*time.Hour).Unix(), 10)},
	}
	server := codexRefusalServer(t, http.StatusTooManyRequests, headers,
		`{"error":{"type":"rate_limit_error","code":"turn_rate_limit_exceeded","message":"Too many turns"}}`)
	err := codexRefusalCalls()[0].call(server.URL)
	if rap, ok := err.(interface{ RetryAfter() *time.Duration }); ok && rap.RetryAfter() != nil {
		t.Fatalf("a turn throttle without Retry-After must not borrow a quota window reset: %v", *rap.RetryAfter())
	}
}

// The manager must cool the model for the provider's Retry-After, not its generic backoff.
func TestCodexHTTPRetryAfterDrivesManagerCooldown(t *testing.T) {
	server := codexRefusalServer(t, http.StatusTooManyRequests, http.Header{"Retry-After": []string{"120"}},
		`{"error":{"type":"rate_limit_error","code":"turn_rate_limit_exceeded","message":"Too many turns"}}`)
	manager := cliproxyauth.NewManager(nil, nil, nil)
	manager.SetRetryConfig(0, 0, 0)
	manager.RegisterExecutor(NewCodexExecutor(&config.Config{}))
	auth := &cliproxyauth.Auth{ID: "codex-retry-after-manager", Provider: "codex",
		Attributes: map[string]string{"base_url": server.URL, "api_key": "synthetic-test"}}
	reg := registry.GetGlobalRegistry()
	reg.RegisterClient(auth.ID, "codex", []*registry.ModelInfo{{ID: "gpt-5.4"}})
	t.Cleanup(func() { reg.UnregisterClient(auth.ID) })
	if _, err := manager.Register(context.Background(), auth); err != nil {
		t.Fatalf("register auth: %v", err)
	}
	start := time.Now()
	_, err := manager.Execute(context.Background(), []string{"codex"}, cliproxyexecutor.Request{
		Model: "gpt-5.4", Payload: []byte(`{"model":"gpt-5.4","input":"synthetic"}`),
	}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("openai-response")})
	if err == nil {
		t.Fatal("expected the throttle to reach the caller")
	}
	updated, ok := manager.GetByID(auth.ID)
	if !ok || updated.ModelStates["gpt-5.4"] == nil {
		t.Fatal("model state missing")
	}
	next := updated.ModelStates["gpt-5.4"].NextRetryAfter
	if next.Before(start.Add(119*time.Second)) || next.After(time.Now().Add(121*time.Second)) {
		t.Fatalf("model cooldown until %v; want start+120s (%v)", next, start.Add(120*time.Second))
	}
}
