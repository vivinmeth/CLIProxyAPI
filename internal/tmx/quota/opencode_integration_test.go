package quota

import (
	"context"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/opencode"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

// OpenCode Go readings become quota accounts beside the subscription logins, are written to
// history, never reach routing, and leave the other accounts as they were.
func TestQuotaOpenCodeAccounts(t *testing.T) {
	clock := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	claude := &coreauth.Auth{ID: "claude-oc.json", Provider: "claude"}
	f := &fakeFetcher{auths: []*coreauth.Auth{claude}, usage: usageJSON(10, 20, 30), status: 200, calls: map[string]int{}}
	s := openTestService(t, f, &clock)
	s.accounts = map[string]*account{}
	obs := &recordingObserver{}
	s.observer = obs
	for _, stmt := range []string{
		`DELETE FROM ` + s.table,
		`CREATE TABLE IF NOT EXISTS tmxq.requests (id BIGSERIAL PRIMARY KEY, ts TIMESTAMPTZ NOT NULL, provider TEXT NOT NULL,
			model TEXT NOT NULL, allowance_usd DOUBLE PRECISION, shadowed BOOLEAN NOT NULL DEFAULT FALSE)`,
		`DELETE FROM tmxq.requests`,
		`DELETE FROM tmxq.opencode_quota`,
	} {
		if _, err := s.db.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}
	ctx := context.Background()
	if _, err := s.db.Exec(`INSERT INTO tmxq.requests (ts, provider, model, allowance_usd) VALUES ($1, 'openai-compatible-opencode-go', 'kimi-k3', 1.5)`,
		clock.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.OpenCode().Put(ctx, opencode.Settings{Entry: "opencode-go", Plan: "go",
		Plans: map[string]map[string]float64{"go": {"kimi-k3": 15}}}); err != nil {
		t.Fatal(err)
	}
	s.tick(ctx)
	byID := map[string]Account{}
	for _, a := range s.Accounts() {
		byID[a.AuthID] = a
	}
	if _, ok := byID["claude-oc.json"]; !ok || len(byID) != 2 {
		t.Fatalf("accounts = %+v", byID)
	}
	oc, ok := byID["opencode:opencode-go:kimi-k3"]
	if !ok || oc.Provider != "opencode-go" || oc.Plan != "go" || oc.Label != "kimi-k3" || len(oc.Windows) != 3 {
		t.Fatalf("opencode account = %+v", oc)
	}
	for _, w := range oc.Windows {
		if w.Window.Window == "5h" && (w.UsedPct == nil || *w.UsedPct != 50 || w.ResetsAt != nil) {
			t.Fatalf("5h window = %+v", w)
		}
	}
	for _, id := range obs.ids {
		if id == "opencode:opencode-go:kimi-k3" {
			t.Fatal("an OpenCode reading reached routing")
		}
	}
	hist, _, err := s.History(ctx, "opencode:opencode-go:kimi-k3", "", clock.Add(-time.Hour), clock.Add(time.Hour), 100)
	if err != nil || len(hist) != 3 {
		t.Fatalf("history = %d %v", len(hist), err)
	}
	// Settings removed: the account drops out at the next tick, its history stays.
	if _, err = s.OpenCode().Delete(ctx, "opencode-go"); err != nil {
		t.Fatal(err)
	}
	clock = clock.Add(time.Minute)
	s.tick(ctx)
	for _, a := range s.Accounts() {
		if a.AuthID == "opencode:opencode-go:kimi-k3" {
			t.Fatal("account kept after its settings were removed")
		}
	}
}

type recordingObserver struct{ ids []string }

func (r *recordingObserver) ApplyQuotaObservation(authID, _ string, _ []coreauth.QuotaWindowObservation) bool {
	r.ids = append(r.ids, authID)
	return true
}
