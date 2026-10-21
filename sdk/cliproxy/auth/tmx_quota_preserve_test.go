package auth

import (
	"context"
	"testing"
	"time"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

func claudePoolAuth(id, account string) *Auth {
	return &Auth{ID: id, Provider: "claude", Status: StatusActive, Metadata: map[string]any{"type": "claude", "account_uuid": account, "access_token": "synthetic-" + id}}
}

// pollPool registers three Claude accounts and applies a usage-API reading to each: 01 is at
// 97% of its 5h window but has the soonest weekly reset, so only the reading keeps it last.
func pollPool(t *testing.T) *Manager {
	t.Helper()
	m := NewManager(nil, &ResetSoonestSelector{}, nil)
	now := time.Now().UTC().Truncate(time.Second)
	for i, id := range []string{"poll-01", "poll-02", "poll-03"} {
		if _, err := m.Register(WithSkipPersist(context.Background()), claudePoolAuth(id, "account-"+id)); err != nil {
			t.Fatal(err)
		}
		used := 10.0
		if id == "poll-01" {
			used = 97
		}
		weekly := 10.0
		if !m.ApplyQuotaObservation(id, "claude", []QuotaWindowObservation{
			{Window: "5h", UsedPercent: &used, ResetsAt: now.Add(2 * time.Hour), ObservedAt: now},
			{Window: "7d", UsedPercent: &weekly, ResetsAt: now.Add(time.Duration(24*(i+1)) * time.Hour), ObservedAt: now},
		}) {
			t.Fatalf("poll for %s not applied", id)
		}
	}
	return m
}

func pickPool(t *testing.T, m *Manager) string {
	t.Helper()
	got, err := (&ResetSoonestSelector{}).Pick(m.PoolSelectionContext(context.Background()), "claude", "", cliproxyexecutor.Options{}, m.List())
	if err != nil {
		t.Fatal(err)
	}
	return got.ID
}

// tmx (load test B3): the watcher's bootstrap re-sync of an auth must not erase the usage-API
// reading the quota poller applied a moment earlier; otherwise the 97% account is picked first.
func TestQuotaPollSurvivesWatcherUpdate(t *testing.T) {
	m := pollPool(t)
	if got := pickPool(t, m); got != "poll-02" {
		t.Fatalf("before the re-sync picked %s, want poll-02", got)
	}
	for _, id := range []string{"poll-01", "poll-02", "poll-03"} {
		// What the watcher hands over: the auth file contents, no runtime quota state.
		if _, err := m.Update(WithSkipPersist(context.Background()), claudePoolAuth(id, "account-"+id)); err != nil {
			t.Fatal(err)
		}
	}
	if got := pickPool(t, m); got == "poll-01" {
		t.Fatal("watcher update erased the polled 97% reading: poll-01 picked first")
	}
	a, _ := m.GetByID("poll-01")
	if a.Quota.Signals["Anthropic-Ratelimit-Unified-5h-Utilization"] != "0.97" {
		t.Fatalf("5h reading lost: %v", a.Quota.Signals)
	}
}

func TestQuotaPollDroppedWhenAccountChanges(t *testing.T) {
	m := pollPool(t)
	if _, err := m.Update(WithSkipPersist(context.Background()), claudePoolAuth("poll-01", "another-account")); err != nil {
		t.Fatal(err)
	}
	a, _ := m.GetByID("poll-01")
	if len(a.Quota.Signals) != 0 {
		t.Fatalf("another account's reading kept: %v", a.Quota.Signals)
	}
}

// A reset credit's clear watermark survives the re-sync, so a reading taken before the redeem
// cannot come back through the preserved state or a late poll.
func TestQuotaClearWatermarkSurvivesWatcherUpdate(t *testing.T) {
	m := pollPool(t)
	cleared := time.Now().UTC().Add(time.Second)
	m.mu.Lock()
	a := m.auths["poll-01"]
	a.Quota.ClearObservationSignals()
	a.Quota.markSignalsCleared([]string{"anthropic-ratelimit-unified-5h-"}, cleared)
	m.mu.Unlock()
	if _, err := m.Update(WithSkipPersist(context.Background()), claudePoolAuth("poll-01", "account-poll-01")); err != nil {
		t.Fatal(err)
	}
	used := 97.0
	m.ApplyQuotaObservation("poll-01", "claude", []QuotaWindowObservation{{Window: "5h", UsedPercent: &used, ResetsAt: time.Now().Add(time.Hour), ObservedAt: cleared.Add(-time.Second)}})
	got, _ := m.GetByID("poll-01")
	if v := got.Quota.Signals["Anthropic-Ratelimit-Unified-5h-Utilization"]; v != "" {
		t.Fatalf("pre-reset reading merged after the re-sync: %q", v)
	}
	if got.Quota.SignalClearedAt["anthropic-ratelimit-unified-5h-"].IsZero() {
		t.Fatal("clear watermark lost on update")
	}
}
