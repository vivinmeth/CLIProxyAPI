package auth

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	executor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

type poolAdmissionExecutor struct {
	unauthorizedRefreshExecutor
	entered      chan string
	release      chan struct{}
	panicExecute bool
	failExecute  bool
	stream       chan executor.StreamChunk
}

func (e *poolAdmissionExecutor) Execute(ctx context.Context, a *Auth, _ executor.Request, _ executor.Options) (executor.Response, error) {
	if e.panicExecute {
		panic("test executor panic")
	}
	if e.failExecute {
		return executor.Response{}, &Error{HTTPStatus: 400, Message: "invalid request"}
	}
	e.entered <- a.ID
	select {
	case <-ctx.Done():
		return executor.Response{}, ctx.Err()
	case <-e.release:
		return executor.Response{Payload: []byte(a.ID)}, nil
	}
}
func (e *poolAdmissionExecutor) ExecuteStream(ctx context.Context, a *Auth, _ executor.Request, _ executor.Options) (*executor.StreamResult, error) {
	e.entered <- a.ID
	return &executor.StreamResult{Chunks: e.stream}, nil
}

func admissionFixture(t *testing.T, n, cap int, unknown, affinity bool) (*Manager, *poolAdmissionExecutor) {
	t.Helper()
	var selector Selector = &ResetSoonestSelector{}
	if affinity {
		selector = NewSessionAffinitySelector(selector)
	}
	m := NewManager(nil, selector, nil)
	cfg := &config.Config{}
	cfg.Routing.ResetSoonest.MaxInflightPerCredential = cap
	m.SetConfig(cfg)
	e := &poolAdmissionExecutor{unauthorizedRefreshExecutor: unauthorizedRefreshExecutor{id: "claude"}, entered: make(chan string, 200), release: make(chan struct{}), stream: make(chan executor.StreamChunk, 1)}
	m.RegisterExecutor(e)
	now := time.Now()
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("admission-%s-%d", t.Name(), i)
		a := &Auth{ID: id, Provider: "claude", Status: StatusActive}
		if !unknown {
			a.Quota.ObserveResponseHeadersForProvider("claude", claudeWeeklyHeaders(now.Add(time.Duration(i+1)*time.Hour), time.Time{}), now)
		}
		if _, err := m.Register(context.Background(), a); err != nil {
			t.Fatal(err)
		}
		registry.GetGlobalRegistry().RegisterClient(id, "claude", []*registry.ModelInfo{{ID: "claude-sonnet-5"}, {ID: "claude-fable-5"}})
		t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(id) })
	}
	return m, e
}
func assertNoReservations(t *testing.T, m *Manager) {
	t.Helper()
	m.tmxAdmission.counts.Range(func(id, value any) bool {
		if got := m.tmxAdmission.counter(id.(string)).Load(); got != 0 {
			t.Errorf("%s leaked %d reservations", id, got)
		}
		return true
	})
}
func coldOptions(i int) executor.Options {
	return executor.Options{Headers: http.Header{"X-Claude-Code-Session-Id": {fmt.Sprintf("cold-%d", i)}}, OriginalRequest: []byte(fmt.Sprintf(`{"session_id":"cold-%d"}`, i))}
}

func TestResetSoonestConcurrentAdmission(t *testing.T) {
	for _, n := range []int{2, 5} {
		for _, unknown := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d/unknown=%v", n, unknown), func(t *testing.T) {
				m, e := admissionFixture(t, n, 100/n, unknown, true)
				var wg sync.WaitGroup
				for i := 0; i < 100; i++ {
					wg.Add(1)
					go func(i int) {
						defer wg.Done()
						_, err := m.Execute(context.Background(), []string{"claude"}, executor.Request{Model: "claude-sonnet-5"}, coldOptions(i))
						if err != nil {
							t.Error(err)
						}
					}(i)
				}
				counts := map[string]int{}
				deadline := time.NewTimer(10 * time.Second)
				defer deadline.Stop()
				for i := 0; i < 100; i++ {
					select {
					case id := <-e.entered:
						counts[id]++
					case <-deadline.C:
						t.Fatal("requests failed to enter")
					}
				}
				close(e.release)
				wg.Wait()
				if len(counts) != n {
					t.Fatalf("distribution %v", counts)
				}
				for _, count := range counts {
					if count > 100/n {
						t.Errorf("cap exceeded: %v", counts)
					}
				}
				assertNoReservations(t, m)
			})
		}
	}
}

func TestResetSoonestSpillPreservesBinding(t *testing.T) {
	m, e := admissionFixture(t, 2, 1, false, true)
	opts := coldOptions(1)
	done := make(chan error, 1)
	go func() {
		_, err := m.Execute(context.Background(), []string{"claude"}, executor.Request{Model: "claude-sonnet-5"}, opts)
		done <- err
	}()
	original := <-e.entered
	// Bound account is at cap: this request uses the other account transiently.
	go func() { <-e.entered; close(e.release) }()
	response, err := m.Execute(context.Background(), []string{"claude"}, executor.Request{Model: "claude-sonnet-5"}, opts)
	if err != nil || string(response.Payload) == original {
		t.Fatalf("did not spill: %s %v", response.Payload, err)
	}
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	response, err = m.Execute(context.Background(), []string{"claude"}, executor.Request{Model: "claude-sonnet-5"}, opts)
	if err != nil || string(response.Payload) != original {
		t.Fatalf("binding changed after transient spill: %s %v want %s", response.Payload, err, original)
	}
	assertNoReservations(t, m)
}

func TestResetSoonestReservationLifetime(t *testing.T) {
	t.Run("panic", func(t *testing.T) {
		m, e := admissionFixture(t, 1, 1, false, false)
		e.panicExecute = true
		func() {
			defer func() {
				if recover() == nil {
					t.Error("expected test panic")
				}
			}()
			_, _ = m.Execute(context.Background(), []string{"claude"}, executor.Request{Model: "claude-sonnet-5"}, executor.Options{})
		}()
		assertNoReservations(t, m)
	})
	t.Run("error", func(t *testing.T) {
		m, e := admissionFixture(t, 1, 1, false, false)
		e.failExecute = true
		if _, err := m.Execute(context.Background(), []string{"claude"}, executor.Request{Model: "claude-sonnet-5"}, executor.Options{}); err == nil {
			t.Fatal("expected failure")
		}
		assertNoReservations(t, m)
	})
	for _, cancelStream := range []bool{false, true} {
		t.Run(fmt.Sprintf("stream-cancel=%v", cancelStream), func(t *testing.T) {
			m, e := admissionFixture(t, 1, 1, false, false)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			e.stream <- executor.StreamChunk{Payload: []byte("data: ok\n\n")}
			stream, err := m.ExecuteStream(ctx, []string{"claude"}, executor.Request{Model: "claude-sonnet-5"}, executor.Options{})
			if err != nil {
				t.Fatal(err)
			}
			id := <-e.entered
			if m.tmxAdmission.counter(id).Load() != 1 {
				t.Fatal("stream released before completion")
			}
			if cancelStream {
				cancel()
				// Prove cancellation frees admission while upstream still has not closed.
				deadline := time.Now().Add(time.Second)
				for m.tmxAdmission.counter(id).Load() != 0 {
					if time.Now().After(deadline) {
						t.Fatal("cancel leaked reservation")
					}
					runtime.Gosched()
				}
				assertNoReservations(t, m)
			}
			close(e.stream)
			for range stream.Chunks {
			}
			assertNoReservations(t, m)
		})
	}
	t.Run("cancel", func(t *testing.T) {
		m, e := admissionFixture(t, 1, 1, false, false)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			_, err := m.Execute(ctx, []string{"claude"}, executor.Request{Model: "claude-sonnet-5"}, executor.Options{})
			done <- err
		}()
		<-e.entered
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("cancel: %v", err)
		}
		assertNoReservations(t, m)
	})
}

func TestResetSoonestThresholdsProbesAndModelWindows(t *testing.T) {
	now := resetSoonestTestNow
	a := observedAuth(t, "a", "claude", claudeWeeklyHeaders(now.Add(time.Hour), now.Add(30*time.Minute)), now)
	b := observedAuth(t, "b", "claude", claudeWeeklyHeaders(now.Add(2*time.Hour), time.Time{}), now)
	a.Quota.ObserveResponseHeadersForProvider("claude", http.Header{"Anthropic-Ratelimit-Unified-5h-Utilization": {"0.95"}}, now)
	if got := pickResetSoonestForModel([]*Auth{a, b}, now, "claude-sonnet-5", admissionView(nil)); got.ID != "b" {
		t.Fatal("5h threshold ignored")
	}
	a.Quota.ObserveResponseHeadersForProvider("claude", http.Header{"Anthropic-Ratelimit-Unified-5h-Utilization": {"0.1"}, "Anthropic-Ratelimit-Unified-7d-Utilization": {"0.98"}}, now)
	if got := pickResetSoonestForModel([]*Auth{a, b}, now, "claude-sonnet-5", admissionView(nil)); got.ID != "b" {
		t.Fatal("weekly threshold ignored")
	}
	if got := pickResetSoonestForModel([]*Auth{a}, now, "claude-sonnet-5", admissionView(nil)); got != a {
		t.Fatal("threshold removed last resort")
	}
	if reset, _ := weeklyResetAtForModel(a, now, "claude-sonnet-5"); !reset.Equal(now.Add(time.Hour)) {
		t.Fatal("Fable reset used for Sonnet")
	}
	if reset, _ := weeklyResetAtForModel(a, now, "claude-fable-5"); !reset.Equal(now.Add(30 * time.Minute)) {
		t.Fatal("Fable reset ignored")
	}
	codex := observedAuth(t, "c", "codex", http.Header{"X-Codex-Secondary-Window-Minutes": {"300"}, "X-Codex-Secondary-Reset-At": {unixString(now.Add(time.Hour))}}, now)
	if _, ok := weeklyResetAtForModel(codex, now, "gpt"); ok {
		t.Fatal("short secondary treated as weekly")
	}
	unknown := &Auth{ID: "unknown"}
	state := &tmxAdmission{}
	state.counter(unknown.ID).Add(1)
	v := tmxAdmissionView{state: state, five: .95, seven: .98}
	if got := pickResetSoonestForModel([]*Auth{unknown, b}, now, "claude-sonnet-5", v); got != b {
		t.Fatal("busy unknown monopolized healthy account")
	}
}

func TestResetSoonestLiveConfigAndModelContext(t *testing.T) {
	m, _ := admissionFixture(t, 2, 0, false, false)
	auths := m.List()
	sort.Slice(auths, func(i, j int) bool { return auths[i].ID < auths[j].ID })
	now := time.Now()
	a := auths[0]
	a.Quota.ObserveResponseHeadersForProvider("claude", http.Header{"Anthropic-Ratelimit-Unified-5h-Utilization": {"0.9"}, "Anthropic-Ratelimit-Unified-5h-Reset": {unixString(now.Add(time.Hour))}, "Anthropic-Ratelimit-Unified-7d-Reset": {unixString(now.Add(4 * time.Hour))}, "Anthropic-Ratelimit-Unified-7d_oi-Reset": {unixString(now.Add(time.Hour))}}, now)
	_, err := m.Update(context.Background(), a)
	if err != nil {
		t.Fatal(err)
	}
	sonnet, err := m.SelectAuth(context.Background(), "claude", "claude-sonnet-5", executor.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if sonnet.ID == a.ID {
		t.Fatal("manager used Fable reset for Sonnet")
	}
	fable, err := m.SelectAuth(context.Background(), "claude", "claude-fable-5", executor.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if fable.ID != a.ID {
		t.Fatal("manager lost model before ranking")
	}
	cfg := &config.Config{}
	cfg.Routing.ResetSoonest.FiveHourThreshold = .8
	m.SetConfig(cfg)
	fable, err = m.SelectAuth(context.Background(), "claude", "claude-fable-5", executor.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if fable.ID == a.ID {
		t.Fatal("threshold hot reload ignored")
	}
}
