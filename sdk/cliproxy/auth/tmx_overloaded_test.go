package auth

import (
	"context"
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

func markOverloaded(t *testing.T, model string, retryAfter *time.Duration) *Auth {
	t.Helper()
	withQuotaCooldownEnabled(t)
	manager := NewManager(nil, nil, nil)
	auth := &Auth{ID: "overloaded-" + model + "-" + time.Now().Format("150405.000000000"), Provider: "claude"}
	if _, err := manager.Register(WithSkipPersist(context.Background()), auth); err != nil {
		t.Fatalf("register auth: %v", err)
	}
	manager.MarkResult(context.Background(), Result{
		AuthID:     auth.ID,
		Provider:   auth.Provider,
		Model:      model,
		RetryAfter: retryAfter,
		Error:      &Error{HTTPStatus: statusOverloaded, Message: "overloaded"},
	})
	updated, ok := manager.GetByID(auth.ID)
	if !ok || updated == nil {
		t.Fatal("auth missing")
	}
	return updated
}

func TestOverloadedGetsShortTransientCooldown(t *testing.T) {
	start := time.Now()
	state := markOverloaded(t, "claude-opus-5", nil).ModelStates["claude-opus-5"]
	if state == nil {
		t.Fatal("model state missing")
	}
	if !state.Unavailable || state.NextRetryAfter.After(time.Now().Add(overloadedCooldown)) || state.NextRetryAfter.Before(start.Add(overloadedCooldown)) {
		t.Fatalf("529 cooldown until %v; want %v, not the generic %v", state.NextRetryAfter, overloadedCooldown, transientErrorCooldown)
	}
	if state.Quota.Exceeded {
		t.Fatal("an overload is not a quota exhaustion")
	}

	hint := 3 * time.Second
	state = markOverloaded(t, "claude-opus-5", &hint).ModelStates["claude-opus-5"]
	if state.NextRetryAfter.After(time.Now().Add(hint)) {
		t.Fatalf("a Retry-After hint must win: cooldown until %v", state.NextRetryAfter)
	}

	auth := markOverloaded(t, "", nil)
	if auth.NextRetryAfter.After(time.Now().Add(overloadedCooldown)) || auth.StatusMessage != "transient upstream error" {
		t.Fatalf("auth-level 529: until %v, message %q", auth.NextRetryAfter, auth.StatusMessage)
	}
}

func TestOverloadedJoinsRetryRoundsLike503(t *testing.T) {
	if !isCredentialRetryRoundStatus(http.StatusServiceUnavailable) || !isCredentialRetryRoundStatus(statusOverloaded) {
		t.Fatal("529 must get the same extra retry round as 503")
	}
	if got := cooldownErrorReason(&Error{HTTPStatus: statusOverloaded}); got != "transient_error" {
		t.Fatalf("cooldown reason = %q, want transient_error", got)
	}
}

// statusPoolExecutor fails every call with one status and counts the calls.
type statusPoolExecutor struct {
	status int
	calls  atomic.Int32
}

func (e *statusPoolExecutor) Identifier() string { return "claude" }
func (e *statusPoolExecutor) Execute(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	e.calls.Add(1)
	return cliproxyexecutor.Response{}, &Error{HTTPStatus: e.status, Message: http.StatusText(e.status)}
}
func (e *statusPoolExecutor) ExecuteStream(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	e.calls.Add(1)
	return nil, &Error{HTTPStatus: e.status, Message: http.StatusText(e.status)}
}
func (e *statusPoolExecutor) Refresh(_ context.Context, a *Auth) (*Auth, error) { return a, nil }
func (e *statusPoolExecutor) CountTokens(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, &Error{HTTPStatus: http.StatusNotImplemented}
}
func (e *statusPoolExecutor) HttpRequest(context.Context, *Auth, *http.Request) (*http.Response, error) {
	return nil, nil
}

func statusPool(t *testing.T, status, n int) (*Manager, *statusPoolExecutor) {
	t.Helper()
	withQuotaCooldownEnabled(t)
	m := NewManager(nil, &ResetSoonestSelector{}, nil)
	m.SetRetryConfig(0, 0, 0)
	e := &statusPoolExecutor{status: status}
	m.RegisterExecutor(e)
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("status-pool-%s-%d", t.Name(), i)
		if _, err := m.Register(context.Background(), &Auth{ID: id, Provider: "claude", Status: StatusActive}); err != nil {
			t.Fatal(err)
		}
		registry.GetGlobalRegistry().RegisterClient(id, "claude", []*registry.ModelInfo{{ID: "claude-opus-5"}})
		t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(id) })
	}
	return m, e
}

func TestOverloadPacesRequestsWhileProviderBacksOff(t *testing.T) {
	m, e := statusPool(t, statusOverloaded, 5)
	req := cliproxyexecutor.Request{Model: "claude-opus-5"}
	if _, err := m.Execute(context.Background(), []string{"claude"}, req, cliproxyexecutor.Options{}); statusCodeFromError(err) != statusOverloaded {
		t.Fatalf("first request: %v", err)
	}
	if got := e.calls.Load(); got != overloadCredentialsPerRound {
		t.Fatalf("first request tried %d credentials, want %d", got, overloadCredentialsPerRound)
	}
	// A parallel request during the provider's overload backoff tries one credential.
	if _, err := m.Execute(context.Background(), []string{"claude"}, req, cliproxyexecutor.Options{}); statusCodeFromError(err) != statusOverloaded {
		t.Fatalf("second request: %v", err)
	}
	if got := e.calls.Load(); got != overloadCredentialsPerRound+1 {
		t.Fatalf("second request tried %d credentials during the backoff, want 1", got-overloadCredentialsPerRound)
	}
}

func TestOverloadPacingKeepsQuotaFailover(t *testing.T) {
	m, e := statusPool(t, http.StatusTooManyRequests, 5)
	if _, err := m.Execute(context.Background(), []string{"claude"}, cliproxyexecutor.Request{Model: "claude-opus-5"}, cliproxyexecutor.Options{}); err == nil {
		t.Fatal("want the pool's 429")
	}
	if got := e.calls.Load(); got != 5 {
		t.Fatalf("a 429 must still fail over across the pool: %d of 5 tried", got)
	}
}

// A client request waits on overload pacing for at most routing.overload-max-wait in total, then
// gets the 529 with the route's remaining backoff as Retry-After.
func TestOverloadWaitBoundedByOverloadMaxWait(t *testing.T) {
	cases := []struct {
		name          string
		maxWait       string
		transient     int
		wantCalls     int32
		maxElapsed    time.Duration
		maxRetryAfter string
	}{
		// The 10 s default overload cooldown never fits the 5 s default budget.
		{name: "default-cooldown-exceeds-budget", wantCalls: 1, maxElapsed: time.Second, maxRetryAfter: "10"},
		{name: "zero-budget", maxWait: "0", transient: 1, wantCalls: 1, maxElapsed: time.Second, maxRetryAfter: "1"},
		// One ~1 s wait fits 1.5 s; the second does not.
		{name: "one-wait-fits", maxWait: "1500ms", transient: 1, wantCalls: 2, maxElapsed: 2500 * time.Millisecond, maxRetryAfter: "1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m, e := statusPool(t, statusOverloaded, 1)
			m.SetRetryConfig(3, 30*time.Second, 0)
			SetTransientErrorCooldownSeconds(tc.transient)
			t.Cleanup(func() { SetTransientErrorCooldownSeconds(0) })
			cfg := &internalconfig.Config{}
			cfg.Routing.OverloadMaxWait = tc.maxWait
			m.SetConfig(cfg)
			start := time.Now()
			_, err := m.Execute(context.Background(), []string{"claude"}, cliproxyexecutor.Request{Model: "claude-opus-5"}, cliproxyexecutor.Options{})
			elapsed := time.Since(start)
			if statusCodeFromError(err) != statusOverloaded {
				t.Fatalf("err = %v, want 529", err)
			}
			if got := e.calls.Load(); got != tc.wantCalls {
				t.Fatalf("upstream calls = %d, want %d", got, tc.wantCalls)
			}
			if elapsed > tc.maxElapsed {
				t.Fatalf("request took %v, want <= %v", elapsed, tc.maxElapsed)
			}
			if got := SafeResponseHeaders(err).Get("Retry-After"); got != tc.maxRetryAfter {
				t.Fatalf("Retry-After = %q, want %q (remaining overload backoff, rounded up)", got, tc.maxRetryAfter)
			}
		})
	}
}
