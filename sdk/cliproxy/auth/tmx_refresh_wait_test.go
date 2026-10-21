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
