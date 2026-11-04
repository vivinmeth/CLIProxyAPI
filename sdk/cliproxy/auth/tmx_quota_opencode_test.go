package auth

import (
	"context"
	"testing"
	"time"
)

// OpenCode Go's polled windows (internal/tmx/quota) land on the compat credential and steer
// reset-soonest: soonest weekly reset first, a rate-limited window last.
func TestExternalQuotaOpenCodeWindowsRoute(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	const provider = "openai-compatible-opencode-go"
	m := NewManager(nil, &ResetSoonestSelector{}, nil)
	for _, id := range []string{"oc-a", "oc-b"} {
		if _, err := m.Register(context.Background(), &Auth{ID: id, Provider: provider, Status: StatusActive}); err != nil {
			t.Fatal(err)
		}
	}
	pct := func(v float64) *float64 { return &v }
	polls := 0
	apply := func(id string, weekly time.Duration, used5h float64, status5h string) {
		t.Helper()
		polls++ // each poll is newer than the last; an older reading never overwrites
		at := now.Add(time.Duration(polls) * time.Second)
		if !m.ApplyQuotaObservation(id, provider, []QuotaWindowObservation{
			{Window: "5h", UsedPercent: pct(used5h), ResetsAt: now.Add(3 * time.Hour), Status: status5h, ObservedAt: at},
			{Window: "7d", UsedPercent: pct(42), ResetsAt: now.Add(weekly), Status: "allowed", ObservedAt: at},
			{Window: "30d", UsedPercent: pct(21), ResetsAt: now.Add(600 * time.Hour), Status: "allowed", ObservedAt: at},
			{Window: "7d:other", UsedPercent: pct(1), ResetsAt: now.Add(time.Hour), ObservedAt: at},
		}) {
			t.Fatalf("%s: observation ignored", id)
		}
	}
	apply("oc-a", 96*time.Hour, 2, "allowed")
	apply("oc-b", 24*time.Hour, 2, "allowed")
	a, _ := m.GetByID("oc-a")
	if a.Quota.Signals["X-Tmx-Quota-7d-Used-Percent"] != "42" || a.Quota.Signals["X-Tmx-Quota-30d-Status"] != "allowed" ||
		a.Quota.Signals["X-Tmx-Quota-5h-Reset"] == "" || len(a.Quota.Signals) != 9 {
		t.Fatalf("signals = %v", a.Quota.Signals)
	}
	if reset, ok := weeklyResetAtForModel(a, now, "kimi-k3"); !ok || !reset.Equal(now.Add(96*time.Hour)) {
		t.Fatalf("weekly reset = %v %v", reset, ok)
	}
	pick := func() string {
		t.Helper()
		var auths []*Auth
		for _, id := range []string{"oc-a", "oc-b"} {
			x, _ := m.GetByID(id)
			auths = append(auths, x)
		}
		return pickResetSoonestForModel(auths, now, "kimi-k3", admissionView(nil)).ID
	}
	if got := pick(); got != "oc-b" {
		t.Fatalf("picked %s, want the sooner weekly reset", got)
	}
	// An ordinary OpenCode response (no quota headers) keeps the polled windows.
	m.MarkResult(context.Background(), Result{AuthID: "oc-b", Provider: provider, Model: "kimi-k3", Success: true})
	if b, _ := m.GetByID("oc-b"); b.Quota.Signals["X-Tmx-Quota-7d-Reset"] == "" {
		t.Fatalf("a response dropped the polled windows: %v", b.Quota.Signals)
	}
	if got := pick(); got != "oc-b" {
		t.Fatalf("picked %s after a response, want the sooner weekly reset", got)
	}
	// OpenCode says oc-b's 5h window is spent (its rounded percent may read low): oc-a first.
	apply("oc-b", 24*time.Hour, 99, "rejected")
	if got := pick(); got != "oc-a" {
		t.Fatalf("picked %s, want the account OpenCode isn't refusing", got)
	}
	// Over the 5h soft threshold without OpenCode's refusal is last resort too.
	apply("oc-b", 24*time.Hour, 2, "allowed")
	apply("oc-a", 96*time.Hour, 96, "allowed")
	if got := pick(); got != "oc-b" {
		t.Fatalf("picked %s, want the account with 5h headroom", got)
	}
	// Other API-key providers still ignore external windows.
	if _, err := m.Register(context.Background(), &Auth{ID: "gem", Provider: "gemini", Status: StatusActive}); err != nil {
		t.Fatal(err)
	}
	if m.ApplyQuotaObservation("gem", "gemini", []QuotaWindowObservation{{Window: "7d", ResetsAt: now.Add(time.Hour), ObservedAt: now}}) {
		t.Fatal("gemini window applied")
	}
}
