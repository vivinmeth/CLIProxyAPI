package auth

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	executor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

func TestRefresh401PoolWaitsForRejectedTokens(t *testing.T) {
	for _, size := range []int{1, 2, 3} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			m := NewManager(nil, &ResetSoonestSelector{}, nil)
			started, release := make(chan struct{}, size), make(chan struct{})
			e := &unauthorizedRefreshExecutor{id: "claude", tokenInvalid: map[string]struct{}{"old": {}}, onRefresh: func() { started <- struct{}{}; <-release }}
			m.RegisterExecutor(e)
			var refreshes sync.WaitGroup
			for i := 0; i < size; i++ {
				id := fmt.Sprintf("refresh-pool-%d", i)
				_, err := m.Register(context.Background(), &Auth{ID: id, Provider: "claude", Status: StatusActive, Metadata: map[string]any{"access_token": "old", "refresh_token": "rotation", "expired": time.Now().Add(time.Hour).Format(time.RFC3339)}})
				if err != nil {
					t.Fatal(err)
				}
				registry.GetGlobalRegistry().RegisterClient(id, "claude", []*registry.ModelInfo{{ID: "test-model"}})
				defer registry.GetGlobalRegistry().UnregisterClient(id)
				refreshes.Add(1)
				go func() {
					defer refreshes.Done()
					if _, err := m.refreshAuthForRequest(context.Background(), id, "old"); err != nil {
						t.Error(err)
					}
				}()
			}
			for i := 0; i < size; i++ {
				<-started
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if _, _, _, err := m.pickNextMixed(ctx, []string{"claude"}, "test-model", executor.Options{}, nil); !errors.Is(err, context.Canceled) {
				t.Errorf("canceled wait: %v", err)
			}
			results := make(chan error, 100)
			for i := 0; i < 100; i++ {
				go func() {
					_, err := m.Execute(context.Background(), []string{"claude"}, executor.Request{Model: "test-model"}, executor.Options{})
					results <- err
				}()
			}
			// This delay models provider refresh latency, rather than any expiry/TTL boundary.
			timer := time.NewTimer(250 * time.Millisecond)
			select {
			case err := <-results:
				t.Errorf("request returned before refresh: %v", err)
				results <- err
			case <-timer.C:
			}
			timer.Stop()
			close(release)
			for i := 0; i < 100; i++ {
				if err := <-results; err != nil {
					t.Error(err)
				}
			}
			refreshes.Wait()
			if got := e.RefreshCalls(); got != size {
				t.Fatalf("refresh calls %d, want %d", got, size)
			}
		})
	}
}

func TestRefreshLockWaitIsCancelable(t *testing.T) {
	m := NewManager(nil, &ResetSoonestSelector{}, nil)
	started, release := make(chan struct{}, 1), make(chan struct{})
	e := &unauthorizedRefreshExecutor{id: "claude", onRefresh: func() { started <- struct{}{}; <-release }}
	m.RegisterExecutor(e)
	_, err := m.Register(context.Background(), &Auth{ID: "cancel-refresh", Provider: "claude", Status: StatusActive, Metadata: map[string]any{"access_token": "old"}})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := m.refreshAuthForRequest(context.Background(), "cancel-refresh", "old"); done <- err }()
	<-started
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = m.refreshAuthForRequest(ctx, "cancel-refresh", "old")
	close(release)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("refresh acquisition did not cancel: %v", err)
	}
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	if e.RefreshCalls() != 1 {
		t.Fatal("canceled waiter rotated token")
	}
}

// registerRefreshWaitAuth registers one claude auth serving test-model and returns its executor,
// whose refreshes signal started and block until release is closed.
func registerRefreshWaitAuth(t *testing.T, id, token string) (*Manager, *unauthorizedRefreshExecutor, chan struct{}, chan struct{}) {
	t.Helper()
	m := NewManager(nil, &ResetSoonestSelector{}, nil)
	started, release := make(chan struct{}, 4), make(chan struct{})
	e := &unauthorizedRefreshExecutor{id: "claude", tokenInvalid: map[string]struct{}{"old": {}}, onRefresh: func() { started <- struct{}{}; <-release }}
	m.RegisterExecutor(e)
	if _, err := m.Register(context.Background(), &Auth{ID: id, Provider: "claude", Status: StatusActive, Metadata: map[string]any{"access_token": token, "refresh_token": "rotation", "expired": time.Now().Add(time.Hour).Format(time.RFC3339)}}); err != nil {
		t.Fatal(err)
	}
	registry.GetGlobalRegistry().RegisterClient(id, "claude", []*registry.ModelInfo{{ID: "test-model"}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(id) })
	return m, e, started, release
}

func withRefreshWaitBudgetFor(t *testing.T, d time.Duration) {
	t.Helper()
	prev := refreshWaitBudget
	refreshWaitBudget = d
	t.Cleanup(func() { refreshWaitBudget = prev })
}

func TestRefreshWaitIgnoresRefreshOfQuotaCooledAuth(t *testing.T) {
	withQuotaCooldownEnabled(t)
	const id = "refresh-wait-quota"
	m, _, started, release := registerRefreshWaitAuth(t, id, "valid")
	defer close(release)
	retryAfter := time.Hour
	m.MarkResult(context.Background(), Result{AuthID: id, Provider: "claude", Model: "test-model", RetryAfter: &retryAfter, Error: &Error{HTTPStatus: 429, Message: "rate limited"}})
	// A scheduled background refresh of the cooled account is slow.
	go func() { _, _ = m.refreshAuthForRequestAtEpoch(context.Background(), id, "", 0) }()
	<-started

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	start := time.Now()
	_, err := m.Execute(ctx, []string{"claude"}, executor.Request{Model: "test-model"}, executor.Options{})
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("request waited %v on a refresh that cannot lift a quota cooldown", elapsed)
	}
	if err == nil || errors.Is(err, context.DeadlineExceeded) || !isAuthUnavailableError(err) {
		t.Fatalf("want the pool's cooldown error, got %v", err)
	}
}

func TestRefreshWaitTimeoutKeepsPoolError(t *testing.T) {
	withRefreshWaitBudgetFor(t, 100*time.Millisecond)
	const id = "refresh-wait-timeout"
	m, _, started, release := registerRefreshWaitAuth(t, id, "old")
	defer close(release)
	// A 401 rejected the token; its refresh hangs on the token endpoint.
	go func() { _, _ = m.refreshAuthForRequest(context.Background(), id, "old") }()
	<-started

	_, err := m.Execute(context.Background(), []string{"claude"}, executor.Request{Model: "test-model"}, executor.Options{})
	if err == nil || errors.Is(err, context.DeadlineExceeded) || !isAuthUnavailableError(err) {
		t.Fatalf("an expired refresh wait must return the pool's error, got %v", err)
	}
}

func TestRefreshWaitBudgetIsSharedPerRequest(t *testing.T) {
	withRefreshWaitBudgetFor(t, 100*time.Millisecond)
	var held, free tmxRefreshMutex
	held.Lock()
	defer held.Unlock()
	ctx := withRefreshWaitBudget(context.Background())
	if err := held.LockContext(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("contended lock: %v", err)
	}
	start := time.Now()
	if err := held.LockContext(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("contended lock after budget: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
		t.Fatalf("a spent budget still waited %v", elapsed)
	}
	// The same budget also bounds the in-flight refresh wait.
	if waitCtx, done := refreshWaitContext(ctx); waitCtx.Err() == nil {
		done()
		t.Fatal("refresh wait got a fresh budget")
	}
	if err := free.LockContext(ctx); err != nil {
		t.Fatalf("an uncontended lock needs no budget: %v", err)
	}
	free.Unlock()
}

