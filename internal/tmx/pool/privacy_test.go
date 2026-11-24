package pool

import (
	"context"
	"encoding/json"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

func TestPoolMasksIDsAndEmptyProviderLocalPart(t *testing.T) {
	s := Build(context.Background(), []*auth.Auth{{ID: "codex-hash-alice@example.com-prolite.json", Provider: "codex", Label: "claude-@x.com"}}, nil, nil, nil, "")
	raw, _ := json.Marshal(s)
	if strings.Contains(string(raw), "alice@example.com") {
		t.Fatal("email leaked in auth_id")
	}
	if s.Credentials[0].Label != "claude-•••@x•••.com" {
		t.Fatalf("label=%s", s.Credentials[0].Label)
	}
}

func TestPoolRankUsesConfiguredAdmissionThreshold(t *testing.T) {
	now := time.Now()
	m := auth.NewManager(nil, &auth.ResetSoonestSelector{}, nil)
	cfg := &config.Config{}
	cfg.Routing.ResetSoonest.SevenDayThreshold = .5
	m.SetConfig(cfg)
	auths := []*auth.Auth{}
	for i, id := range []string{"a", "b"} {
		a := &auth.Auth{ID: id, Provider: "claude", Status: auth.StatusActive}
		used := "0.6"
		if i == 1 {
			used = "0.1"
		}
		a.Quota.ObserveResponseHeadersForProvider("claude", map[string][]string{"Anthropic-Ratelimit-Unified-7d-Utilization": {used}, "Anthropic-Ratelimit-Unified-7d-Reset": {strconv.FormatInt(now.Add(time.Duration(i+1)*time.Hour).Unix(), 10)}}, now)
		auths = append(auths, a)
	}
	s := Build(context.Background(), auths, nil, nil, m, "")
	if s.Credentials[1].PickRank == nil || *s.Credentials[1].PickRank != 1 {
		t.Fatalf("configured threshold ignored: %+v", s.Credentials)
	}
}

func TestPoolCarriesQuotaAuthIndex(t *testing.T) {
	a := &auth.Auth{ID: "claude-hash-alice@example.com.json", Provider: "claude", Status: auth.StatusActive}
	want := a.Clone().EnsureIndex()
	s := Build(context.Background(), []*auth.Auth{a}, nil, nil, nil, "")
	if want == "" || s.Credentials[0].AuthIndex != want {
		t.Fatalf("auth_index=%q want %q", s.Credentials[0].AuthIndex, want)
	}
	raw, _ := json.Marshal(s)
	if !strings.Contains(string(raw), `"auth_index":"`+want+`"`) || strings.Contains(string(raw), "alice@example.com") {
		t.Fatalf("payload=%s", raw)
	}
}

// An OpenCode credential shows the usage API's windows routing keeps on it (no response headers).
func TestPoolShowsPolledOpenCodeWindows(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	const provider = "openai-compatible-opencode-go"
	m := auth.NewManager(nil, &auth.ResetSoonestSelector{}, nil)
	if _, err := m.Register(context.Background(), &auth.Auth{ID: "oc", Provider: provider, Status: auth.StatusActive}); err != nil {
		t.Fatal(err)
	}
	used := 42.0
	m.ApplyQuotaObservation("oc", provider, []auth.QuotaWindowObservation{
		{Window: "7d", UsedPercent: &used, ResetsAt: now.Add(96 * time.Hour), Status: "allowed", ObservedAt: now}})
	a, _ := m.GetByID("oc")
	s := Build(context.Background(), []*auth.Auth{a}, nil, nil, nil, "")
	ws := s.Credentials[0].Windows
	if len(ws) != 1 || ws[0].Window.Window != "7d" || ws[0].UsedPct == nil || *ws[0].UsedPct != 42 ||
		ws[0].ResetsAt == nil || !ws[0].ResetsAt.Equal(now.Add(96*time.Hour)) || ws[0].Source != "polled" {
		t.Fatalf("windows = %+v", ws)
	}
}

// T13: a passive window shows its own field clock, not the account-wide ObservedAt that any newer
// header (here a 5h one) moves forward. A frozen 7d window then shows its real age.
func TestPoolPassiveWindowKeepsItsOwnClock(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	weekly, session := now.Add(-7*time.Hour), now.Add(-time.Minute)
	a := &auth.Auth{ID: "claude-hash-alice@example.com.json", Provider: "claude", Label: "claude-alice@example.com", Status: auth.StatusActive}
	a.Quota.ObservedAt = session
	a.Quota.Signals = map[string]string{
		"Anthropic-Ratelimit-Unified-7d-Utilization": "0.98",
		"Anthropic-Ratelimit-Unified-7d-Reset":       strconv.FormatInt(now.Add(40*time.Hour).Unix(), 10),
		"Anthropic-Ratelimit-Unified-5h-Utilization": "0.1",
		"Anthropic-Ratelimit-Unified-5h-Reset":       strconv.FormatInt(now.Add(2*time.Hour).Unix(), 10),
	}
	a.Quota.SignalObservedAt = map[string]time.Time{
		"Anthropic-Ratelimit-Unified-7d-Utilization": weekly,
		"Anthropic-Ratelimit-Unified-7d-Reset":       weekly,
		"Anthropic-Ratelimit-Unified-5h-Utilization": session,
		"Anthropic-Ratelimit-Unified-5h-Reset":       session,
	}
	s := Build(context.Background(), []*auth.Auth{a}, nil, nil, nil, "")
	got := map[string]time.Time{}
	for _, w := range s.Credentials[0].Windows {
		got[w.Window.Window] = w.ObservedAt
		if w.Source != "passive" {
			t.Fatalf("%s source = %q", w.Window.Window, w.Source)
		}
	}
	if !got["7d"].Equal(weekly) {
		t.Fatalf("7d observed_at = %v, want the 7 h old field clock %v", got["7d"], weekly)
	}
	if !got["5h"].Equal(session) {
		t.Fatalf("5h observed_at = %v, want %v", got["5h"], session)
	}

	// Privacy: the per-window clocks add no raw IDs, labels or signal names to the payload.
	raw, _ := json.Marshal(s)
	for _, leak := range []string{"alice@example.com", "Anthropic-Ratelimit", "signal_observed_at"} {
		if strings.Contains(string(raw), leak) {
			t.Fatalf("pool payload leaks %q: %s", leak, raw)
		}
	}
	if !strings.Contains(string(raw), `"observed_at":"`+weekly.Format(time.RFC3339)) {
		t.Fatalf("payload lacks the 7d window's own observed_at: %s", raw)
	}
}

// A Codex window found by its length keeps its own clock too.
func TestPoolCodexWindowKeepsItsOwnClock(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	weekly, session := now.Add(-3*time.Hour), now.Add(-time.Minute)
	a := &auth.Auth{ID: "codex-a", Provider: "codex", Status: auth.StatusActive}
	a.Quota.ObservedAt = session
	a.Quota.Signals = map[string]string{
		"X-Codex-Primary-Used-Percent":     "10",
		"X-Codex-Primary-Window-Minutes":   "300",
		"X-Codex-Primary-Reset-At":         strconv.FormatInt(now.Add(time.Hour).Unix(), 10),
		"X-Codex-Secondary-Used-Percent":   "50",
		"X-Codex-Secondary-Window-Minutes": "10080",
		"X-Codex-Secondary-Reset-At":       strconv.FormatInt(now.Add(48*time.Hour).Unix(), 10),
	}
	a.Quota.SignalObservedAt = map[string]time.Time{}
	for key := range a.Quota.Signals {
		a.Quota.SignalObservedAt[key] = session
		if strings.HasPrefix(key, "X-Codex-Secondary-") {
			a.Quota.SignalObservedAt[key] = weekly
		}
	}
	s := Build(context.Background(), []*auth.Auth{a}, nil, nil, nil, "")
	for _, w := range s.Credentials[0].Windows {
		want := session
		if w.Window.Window == "7d" {
			want = weekly
		}
		if !w.ObservedAt.Equal(want) {
			t.Fatalf("%s observed_at = %v, want %v", w.Window.Window, w.ObservedAt, want)
		}
	}
	if len(s.Credentials[0].Windows) != 2 {
		t.Fatalf("windows = %+v", s.Credentials[0].Windows)
	}
}
