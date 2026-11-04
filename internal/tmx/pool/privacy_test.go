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
