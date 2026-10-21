package executor

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v8/sdk/translator"
)

// overloadPoolRun sends one client request to a pool of n Claude credentials whose upstream
// answers every call with status and the given headers, and reports the upstream calls, the
// elapsed time and the client error.
func overloadPoolRun(t *testing.T, name string, n int, stream bool, status int, headers http.Header, body string) (int32, time.Duration, error) {
	t.Helper()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		for key, values := range headers {
			w.Header()[key] = values
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)

	m := cliproxyauth.NewManager(nil, &cliproxyauth.ResetSoonestSelector{}, nil)
	// The load-test settings: request-retry 3, max-retry-interval 30s.
	m.SetRetryConfig(3, 30*time.Second, 0)
	m.RegisterExecutor(NewClaudeExecutor(&config.Config{}))
	reg := registry.GetGlobalRegistry()
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("%s-%d-%v-%d", name, n, stream, i)
		reg.RegisterClient(id, "claude", []*registry.ModelInfo{{ID: "claude-opus-5"}})
		t.Cleanup(func() { reg.UnregisterClient(id) })
		if _, err := m.Register(context.Background(), &cliproxyauth.Auth{ID: id, Provider: "claude", Attributes: map[string]string{"base_url": srv.URL, "api_key": "synthetic"}}); err != nil {
			t.Fatal(err)
		}
	}
	req := cliproxyexecutor.Request{Model: "claude-opus-5", Payload: []byte(`{"model":"claude-opus-5","messages":[{"role":"user","content":"hi"}]}`)}
	opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatClaude, Stream: stream}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	start := time.Now()
	var err error
	if stream {
		var result *cliproxyexecutor.StreamResult
		result, err = m.ExecuteStream(ctx, []string{"claude"}, req, opts)
		if err == nil && result != nil {
			for chunk := range result.Chunks {
				if chunk.Err != nil {
					err = chunk.Err
				}
			}
		}
	} else {
		_, err = m.Execute(ctx, []string{"claude"}, req, opts)
	}
	elapsed := time.Since(start)
	for _, a := range m.List() {
		if c := m.InFlightCount(a.ID); c != 0 {
			t.Errorf("reservation leaked: %s=%d", a.ID, c)
		}
	}
	t.Logf("upstream calls=%d elapsed=%v err=%.120v retry-after=%q", calls.Load(), elapsed.Round(time.Millisecond), err, cliproxyauth.SafeResponseHeaders(err).Get("Retry-After"))
	return calls.Load(), elapsed, err
}

func errStatus(err error) int {
	var coder interface{ StatusCode() int }
	if errors.As(err, &coder) && coder != nil {
		return coder.StatusCode()
	}
	return 0
}

const overloadedBody = `{"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}`

// tmx: a 529 with an explicit Retry-After is not a quota window. The client gets the 529 back
// within the overload wait budget (5 s by default) with a short Retry-After, instead of waiting
// on a quota-fuzzed hint across retry rounds (B1: p95 35 s, Retry-After 31).
func TestClaudeOverloadRetryAfterBoundsClientWait(t *testing.T) {
	const budget = 5 * time.Second
	for _, n := range []int{1, 3, 5} {
		for _, stream := range []bool{false, true} {
			t.Run(fmt.Sprintf("credentials=%d/stream=%v", n, stream), func(t *testing.T) {
				t.Parallel()
				calls, elapsed, err := overloadPoolRun(t, "overload-wait", n, stream, 529, http.Header{"Retry-After": {"2"}}, overloadedBody)
				if err == nil {
					t.Fatal("an overloaded pool must fail")
				}
				if got := errStatus(err); got != 529 {
					t.Fatalf("status = %d, want 529", got)
				}
				if elapsed >= budget+time.Second {
					t.Fatalf("client waited %v, want < %v", elapsed, budget+time.Second)
				}
				if calls > 4 {
					t.Fatalf("upstream calls = %d for one request, want at most 4", calls)
				}
				raw := cliproxyauth.SafeResponseHeaders(err).Get("Retry-After")
				seconds, errParse := strconv.Atoi(raw)
				if errParse != nil || seconds < 1 || seconds > 3 {
					t.Fatalf("Retry-After = %q, want 1..3", raw)
				}
			})
		}
	}
}

// tmx: the overload budget leaves quota failover alone: a 429 with a reset hint still waits for
// the soonest credential within max-retry-interval and keeps the quota deadline as Retry-After.
func TestClaudeQuota429FailoverUnchangedByOverloadBudget(t *testing.T) {
	headers := http.Header{
		"Retry-After":                           {"40"},
		"Anthropic-Ratelimit-Unified-Status":    {"rejected"},
		"Anthropic-Ratelimit-Unified-5h-Status": {"rejected"},
	}
	body := `{"type":"error","error":{"type":"rate_limit_error","message":"Your account has exceeded its rate limit."}}`
	for _, n := range []int{1, 3} {
		t.Run(fmt.Sprintf("credentials=%d", n), func(t *testing.T) {
			calls, elapsed, err := overloadPoolRun(t, "quota-unchanged", n, false, 429, headers, body)
			if got := errStatus(err); got != 429 {
				t.Fatalf("status = %d, want 429", got)
			}
			// Every credential is tried once; the 40 s+ deadline exceeds max-retry-interval, so
			// the request ends without waiting.
			if calls != int32(n) {
				t.Fatalf("upstream calls = %d, want %d (one per credential)", calls, n)
			}
			if elapsed > 2*time.Second {
				t.Fatalf("quota refusal took %v, want immediate", elapsed)
			}
			raw := cliproxyauth.SafeResponseHeaders(err).Get("Retry-After")
			seconds, errParse := strconv.Atoi(raw)
			if errParse != nil || seconds < 40 {
				t.Fatalf("Retry-After = %q, want the quota deadline (>= 40)", raw)
			}
		})
	}
}
