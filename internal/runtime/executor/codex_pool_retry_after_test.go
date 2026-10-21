package executor

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v8/sdk/translator"
)

// tmx: when every credential refuses, the client's Retry-After is when the pool recovers (the
// soonest credential), not the delay of whichever credential happened to be tried last.
func TestCodexPoolRefusalRetryAfterIsSoonestRecovery(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		delay := "120"
		if r.Header.Get("Authorization") == "Bearer synthetic-long" {
			delay = "3600"
		}
		w.Header().Set("Retry-After", delay)
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":{"type":"rate_limit_error","code":"turn_rate_limit_exceeded","message":"Too many turns"}}`))
	}))
	defer srv.Close()

	m := cliproxyauth.NewManager(nil, &cliproxyauth.FillFirstSelector{}, nil)
	m.SetRetryConfig(0, 0, 0)
	m.RegisterExecutor(NewCodexExecutor(&config.Config{}))
	for i, key := range []string{"synthetic-short", "synthetic-long"} {
		id := fmt.Sprintf("pool-retry-after-%d", i)
		reg := registry.GetGlobalRegistry()
		reg.RegisterClient(id, "codex", []*registry.ModelInfo{{ID: "gpt-5.4"}})
		defer reg.UnregisterClient(id)
		if _, err := m.Register(context.Background(), &cliproxyauth.Auth{ID: id, Provider: "codex", Attributes: map[string]string{"base_url": srv.URL, "api_key": key}}); err != nil {
			t.Fatal(err)
		}
	}
	_, err := m.Execute(context.Background(), []string{"codex"}, cliproxyexecutor.Request{Model: "gpt-5.4", Payload: []byte(`{"model":"gpt-5.4","input":"synthetic"}`)}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAIResponse})
	if calls.Load() != 2 {
		t.Fatalf("upstream calls = %d, want both credentials tried", calls.Load())
	}
	if got := cliproxyauth.SafeResponseHeaders(err).Get("Retry-After"); got != "120" {
		t.Fatalf("Retry-After = %q, want the pool's soonest recovery 120 (err %v)", got, err)
	}
	// Each credential keeps its own deadline.
	for _, a := range m.List() {
		want := map[string]bool{"pool-retry-after-0": false, "pool-retry-after-1": true}[a.ID]
		long := false
		for _, state := range a.ModelStates {
			long = long || state.NextRetryAfter.Sub(state.UpdatedAt) > 3000e9
		}
		if long != want {
			t.Errorf("%s cooldown changed: %+v", a.ID, a.ModelStates)
		}
	}
}
