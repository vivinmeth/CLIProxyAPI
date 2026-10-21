package executor

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v8/sdk/translator"
)

// tmx: one overloaded (529) client request must not walk the whole pool on every retry round.
func TestClaudeOverloadDoesNotFanOutAcrossPool(t *testing.T) {
	for _, disabled := range []bool{false, true} {
		for _, n := range []int{3, 5} {
			for _, stream := range []bool{false, true} {
				t.Run(fmt.Sprintf("cooldown-disabled=%v/credentials=%d/stream=%v", disabled, n, stream), func(t *testing.T) {
					var calls atomic.Int32
					srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						calls.Add(1)
						w.WriteHeader(529)
						_, _ = w.Write([]byte(`{"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}`))
					}))
					defer srv.Close()
					cliproxyauth.SetQuotaCooldownDisabled(disabled)
					defer cliproxyauth.SetQuotaCooldownDisabled(false)
					cliproxyauth.SetTransientErrorCooldownSeconds(1)
					defer cliproxyauth.SetTransientErrorCooldownSeconds(0)

					m := cliproxyauth.NewManager(nil, &cliproxyauth.ResetSoonestSelector{}, nil)
					m.SetRetryConfig(2, 2*time.Second, 0)
					m.RegisterExecutor(NewClaudeExecutor(&config.Config{}))
					for i := 0; i < n; i++ {
						id := fmt.Sprintf("overload-%v-%d-%v-%d", disabled, n, stream, i)
						reg := registry.GetGlobalRegistry()
						reg.RegisterClient(id, "claude", []*registry.ModelInfo{{ID: "claude-opus-5"}})
						defer reg.UnregisterClient(id)
						if _, err := m.Register(context.Background(), &cliproxyauth.Auth{ID: id, Provider: "claude", Attributes: map[string]string{"base_url": srv.URL, "api_key": "synthetic"}}); err != nil {
							t.Fatal(err)
						}
					}
					req := cliproxyexecutor.Request{Model: "claude-opus-5", Payload: []byte(`{"model":"claude-opus-5","messages":[{"role":"user","content":"hi"}]}`)}
					opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatClaude, Stream: stream}
					start := time.Now()
					var err error
					if stream {
						var result *cliproxyexecutor.StreamResult
						result, err = m.ExecuteStream(context.Background(), []string{"claude"}, req, opts)
						if err == nil && result != nil {
							for chunk := range result.Chunks {
								if chunk.Err != nil {
									err = chunk.Err
								}
							}
						}
					} else {
						_, err = m.Execute(context.Background(), []string{"claude"}, req, opts)
					}
					t.Logf("upstream calls=%d elapsed=%v err=%v", calls.Load(), time.Since(start).Round(time.Millisecond), err)
					if err == nil {
						t.Fatal("an overloaded pool must fail")
					}
					if got := calls.Load(); got > 4 {
						t.Fatalf("upstream calls = %d for one request, want at most 4", got)
					}
					for _, a := range m.List() {
						if c := m.InFlightCount(a.ID); c != 0 {
							t.Errorf("reservation leaked: %s=%d", a.ID, c)
						}
					}
				})
			}
		}
	}
}
