package auth

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
)

type recordingCooldownObserver struct {
	mu       sync.Mutex
	reports  []CooldownReport
	failures []FailureReport
}

func (r *recordingCooldownObserver) ObserveCooldowns(rep CooldownReport) {
	r.mu.Lock()
	r.reports = append(r.reports, rep)
	r.mu.Unlock()
}

func (r *recordingCooldownObserver) ObserveFailure(f FailureReport) {
	r.mu.Lock()
	r.failures = append(r.failures, f)
	r.mu.Unlock()
}

func TestCooldownObserverSeesStartAndClear(t *testing.T) {
	obs := &recordingCooldownObserver{}
	SetCooldownObserver(obs)
	t.Cleanup(func() { SetCooldownObserver(nil) })

	manager := NewManager(nil, nil, nil)
	a := &Auth{ID: uuid.NewString() + "-obs", Provider: "claude", Label: "me@example", Attributes: map[string]string{"api_key": "k"}}
	reg := registry.GetGlobalRegistry()
	reg.RegisterClient(a.ID, "claude", []*registry.ModelInfo{{ID: "claude-x"}})
	t.Cleanup(func() { reg.UnregisterClient(a.ID) })
	if _, err := manager.Register(context.Background(), a); err != nil {
		t.Fatal(err)
	}

	wait := 90 * time.Second
	manager.MarkResult(context.Background(), Result{AuthID: a.ID, Provider: "claude", Model: "claude-x",
		RetryAfter: &wait, Error: &Error{HTTPStatus: http.StatusTooManyRequests, Message: "rate limited"}})

	obs.mu.Lock()
	if len(obs.reports) != 1 || len(obs.failures) != 1 {
		t.Fatalf("reports %d failures %d", len(obs.reports), len(obs.failures))
	}
	rep := obs.reports[0]
	// The model window, plus the credential-wide one (model "") while every model is cooling.
	if rep.AuthID != a.ID || rep.Seq == 0 || len(rep.Windows) == 0 {
		t.Fatalf("report: %+v", rep)
	}
	w := rep.Windows[len(rep.Windows)-1]
	if w.Model != "claude-x" || w.HTTPStatus != 429 || w.AuthLabel != "me@example" || w.AuthIndex == "" ||
		w.RetryAt.Sub(rep.At) < 89*time.Second {
		t.Fatalf("window: %+v", w)
	}
	if f := obs.failures[0]; f.HTTPStatus != 429 || f.RetryAfter == nil || f.AuthIndex == "" {
		t.Fatalf("failure: %+v", f)
	}
	obs.mu.Unlock()

	snap := manager.CooldownSnapshot()
	if snap.Seq < rep.Seq || len(snap.Windows) == 0 {
		t.Fatalf("snapshot: %+v", snap)
	}

	if _, _, err := manager.ResetQuota(context.Background(), a.ID); err != nil {
		t.Fatal(err)
	}
	obs.mu.Lock()
	defer obs.mu.Unlock()
	last := obs.reports[len(obs.reports)-1]
	if last.Seq <= rep.Seq || len(last.Windows) != 0 {
		t.Fatalf("after reset: %+v", last)
	}
}
