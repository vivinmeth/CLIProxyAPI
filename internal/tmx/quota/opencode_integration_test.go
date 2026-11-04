package quota

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/opencode"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/requestlog"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

// OpenCode Go readings become quota accounts beside the subscription logins, are written to
// history, never reach routing under their own id, and leave the other accounts as they were.
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
		`DROP TABLE IF EXISTS tmxq.requests`,
		`CREATE TABLE tmxq.requests (id BIGSERIAL PRIMARY KEY, ts TIMESTAMPTZ NOT NULL, provider TEXT NOT NULL,
			model TEXT NOT NULL, allowance_usd DOUBLE PRECISION, cost_usd DOUBLE PRECISION, price_source TEXT,
			shadowed BOOLEAN NOT NULL DEFAULT FALSE)`,
		`DELETE FROM tmxq.requests`,
		`DELETE FROM tmxq.opencode_quota`,
	} {
		if _, err := s.db.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}
	ctx := context.Background()
	t.Cleanup(func() { _, _ = s.db.Exec(`DELETE FROM tmxq.opencode_quota`) })
	if _, err := s.db.Exec(`INSERT INTO tmxq.requests (ts, provider, model, allowance_usd) VALUES ($1, 'openai-compatible-opencode-go', 'kimi-k3', 1.5)`,
		clock.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.OpenCode().Put(ctx, opencode.Settings{Entry: "opencode-go", Plan: "go",
		Plans: map[string]opencode.Plan{"go": {Pool: 60, Caps: map[string]float64{"kimi-k3": 15}}}}); err != nil {
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
	oc, ok := byID["opencode:opencode-go"]
	if !ok || oc.Provider != "opencode-go" || oc.Plan != "go" || oc.Label != "OpenCode Go" || len(oc.Windows) != 3 {
		t.Fatalf("opencode account = %+v", oc)
	}
	for _, w := range oc.Windows {
		// 1.5 of the 5h window's 12 (20% of 60), resetting 4h from now (opened 1h ago).
		if w.Window.Window == "5h" && (w.UsedPct == nil || *w.UsedPct != 12.5 || w.ResetsAt == nil || !w.ResetsAt.Equal(clock.Add(4*time.Hour))) {
			t.Fatalf("5h window = %+v", w)
		}
	}
	for _, id := range obs.ids {
		if id == "opencode:opencode-go" {
			t.Fatal("an OpenCode reading reached routing")
		}
	}
	hist, _, err := s.History(ctx, "opencode:opencode-go", "", clock.Add(-time.Hour), clock.Add(time.Hour), 100)
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
		if a.AuthID == "opencode:opencode-go" {
			t.Fatal("account kept after its settings were removed")
		}
	}
}

type recordingObserver struct{ ids []string }

func (r *recordingObserver) ApplyQuotaObservation(authID, _ string, _ []coreauth.QuotaWindowObservation) bool {
	r.ids = append(r.ids, authID)
	return true
}

// OpenCode Go's usage API: polled readings win, the allowance logged since the poll is added until
// the next one, a 404 or another shape falls back to the allowance estimate with a poll_error, and
// polled windows reach routing on the account's credential.
func TestQuotaOpenCodePolled(t *testing.T) {
	clock := time.Date(2026, 11, 4, 4, 0, 0, 0, time.UTC)
	const key = "oc_sk_polltest"
	cred := &coreauth.Auth{ID: "oc-cred-1", Provider: "openai-compatible-opencode-go", Attributes: map[string]string{
		"compat_name": "opencode-go", "api_key": key, "base_url": "https://opencode.test/inference/go/openai/v1"}}
	cred2 := &coreauth.Auth{ID: "oc-cred-2", Provider: "openai-compatible-opencode-go", Attributes: map[string]string{
		"compat_name": "opencode-go", "api_key": key + "2", "base_url": "https://opencode2.test/inference/go/openai/v1"}}
	const zen, inference = "https://opencode.test/zen/go/v1/usage", "https://opencode.test/inference/go/v1/usage"
	f := &fakeFetcher{auths: []*coreauth.Auth{cred, cred2}, status: 500, calls: map[string]int{}, routes: map[string]fakeRoute{
		zen: {200, openCodeUsageReply},
		// account-2's API answers another shape.
		"https://opencode2.test/zen/go/v1/usage": {200, `{"usage":{"rolling":{"percent":"2"}}}`},
	}}
	s := openTestService(t, f, &clock)
	s.accounts = map[string]*account{}
	obs := &capturingObserver{}
	s.observer = obs
	for _, stmt := range []string{
		`DELETE FROM ` + s.table,
		`DROP TABLE IF EXISTS tmxq.requests`,
		`CREATE TABLE tmxq.requests (id BIGSERIAL PRIMARY KEY, ts TIMESTAMPTZ NOT NULL, provider TEXT NOT NULL,
			model TEXT NOT NULL, allowance_usd DOUBLE PRECISION, cost_usd DOUBLE PRECISION, price_source TEXT,
			auth_label TEXT, shadowed BOOLEAN NOT NULL DEFAULT FALSE)`,
		`DELETE FROM tmxq.opencode_quota`,
		`DELETE FROM tmxq.opencode_anchors`,
	} {
		if _, err := s.db.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}
	ctx := context.Background()
	t.Cleanup(func() { _, _ = s.db.Exec(`DELETE FROM tmxq.opencode_quota`) })
	fp, fp2 := requestlog.KeyFingerprint(key), requestlog.KeyFingerprint(key+"2")
	if _, err := s.OpenCode().Put(ctx, opencode.Settings{Entry: "opencode-go", Plan: "go",
		Plans:    map[string]opencode.Plan{"go": {Pool: 60, Caps: map[string]float64{"kimi-k3": 15}}},
		Accounts: []opencode.Account{{Name: "account-1", Keys: []string{fp}}, {Name: "account-2", Keys: []string{fp2}}}}); err != nil {
		t.Fatal(err)
	}
	logAllowance := func(label string, usd float64, at time.Time) {
		t.Helper()
		if _, err := s.db.Exec(`INSERT INTO tmxq.requests (ts, provider, model, allowance_usd, auth_label)
			VALUES ($1, 'openai-compatible-opencode-go', 'kimi-k3', $2, $3)`, at, usd, label); err != nil {
			t.Fatal(err)
		}
	}
	// Before the first poll: $6 on account-1 (counted by OpenCode already), $1.5 on account-2.
	logAllowance(fp, 6, clock.Add(-time.Hour))
	logAllowance(fp2, 1.5, clock.Add(-time.Hour))
	view := func(id string) (Account, map[string]Current) {
		t.Helper()
		for _, a := range s.Accounts() {
			if a.AuthID == id {
				ws := map[string]Current{}
				for _, w := range a.Windows {
					ws[w.Window.Window] = w
				}
				return a, ws
			}
		}
		t.Fatalf("no account %s in %+v", id, s.Accounts())
		return Account{}, nil
	}
	expect := func(ws map[string]Current, window string, pct float64, reset, source string) {
		t.Helper()
		w, ok := ws[window]
		if !ok || w.UsedPct == nil || *w.UsedPct != pct || w.Source != source ||
			(reset == "" && w.ResetsAt != nil) || (reset != "" && (w.ResetsAt == nil || w.ResetsAt.Format(time.RFC3339) != reset)) {
			t.Fatalf("%s = %+v (used %v, resets %v), want %v%% %q %s", window, w, w.UsedPct, w.ResetsAt, pct, reset, source)
		}
	}

	s.tick(ctx)
	a1, ws := view("opencode:opencode-go:account-1")
	if a1.PollError != "" || a1.LastPollAt == nil || f.calls[zen] != 1 || f.calls[inference] != 0 {
		t.Fatalf("account-1 = %+v, calls %v", a1, f.calls)
	}
	expect(ws, "5h", 2, "2026-11-04T08:36:27Z", "polled")
	expect(ws, "7d", 42, "2026-11-09T00:00:00Z", "polled")
	expect(ws, "30d", 100, "2026-12-03T13:32:12Z", "polled")
	if ws["30d"].Status != opencode.StatusAtLimit {
		t.Fatalf("30d status = %q", ws["30d"].Status)
	}
	// Polled windows reached routing on the credential, never on the quota account.
	if got := obs.windows["oc-cred-1"]; len(got) != 3 || obs.windows["opencode:opencode-go:account-1"] != nil {
		t.Fatalf("routing = %+v", obs.windows)
	}
	// account-2: another shape is a poll_error, and the allowance estimate stays (1.5 of 12 = 12.5%).
	a2, ws2 := view("opencode:opencode-go:account-2")
	if a2.PollError == "" || !strings.Contains(a2.PollError, "allowance estimate") {
		t.Fatalf("account-2 poll error = %q", a2.PollError)
	}
	expect(ws2, "5h", 12.5, "2026-11-04T08:00:00Z", "passive")
	if obs.windows["oc-cred-2"] != nil {
		t.Fatal("a failed poll reached routing")
	}

	// A burst after the poll shows before the next one: $3 = 25% of 5h, 10% of 7d, 5% of 30d.
	logAllowance(fp, 3, clock.Add(time.Minute))
	clock = clock.Add(2 * time.Minute)
	s.tick(ctx)
	_, ws = view("opencode:opencode-go:account-1")
	if f.calls[zen] != 1 {
		t.Fatalf("polled again early: %v", f.calls)
	}
	expect(ws, "5h", 27, "2026-11-04T08:36:27Z", "passive")
	expect(ws, "7d", 52, "2026-11-09T00:00:00Z", "passive")
	expect(ws, "30d", 105, "2026-12-03T13:32:12Z", "passive")

	// The API goes away (404 on both paths): poll_error, back-off, and the last poll still anchors
	// each window until its reset.
	f.routes[zen] = fakeRoute{404, `{}`}
	f.routes[inference] = fakeRoute{404, `{}`}
	clock = clock.Add(15 * time.Minute)
	s.tick(ctx)
	a1, ws = view("opencode:opencode-go:account-1")
	if !strings.Contains(a1.PollError, "404") || f.calls[inference] != 1 || a1.NextPollAt == nil || a1.NextPollAt.Before(clock.Add(59*time.Minute)) {
		t.Fatalf("after 404: %+v calls %v", a1, f.calls)
	}
	expect(ws, "7d", 52, "2026-11-09T00:00:00Z", "passive")
	// Past the polled 5h reset, that window is the allowance estimate again.
	clock = time.Date(2026, 11, 4, 8, 40, 0, 0, time.UTC)
	s.tick(ctx)
	_, ws = view("opencode:opencode-go:account-1")
	if w := ws["5h"]; w.Source != "passive" || (w.ResetsAt != nil && w.ResetsAt.Format(time.RFC3339) == "2026-11-04T08:36:27Z") {
		t.Fatalf("5h past its polled reset = %+v", w)
	}
	expect(ws, "7d", 52, "2026-11-09T00:00:00Z", "passive")

	// The second path answers: polled again (back-off over), readings replaced.
	f.routes[inference] = fakeRoute{200, strings.ReplaceAll(openCodeUsageReply, `"percent":42`, `"percent":55`)}
	clock = clock.Add(2 * time.Hour)
	s.tick(ctx)
	a1, ws = view("opencode:opencode-go:account-1")
	if a1.PollError != "" {
		t.Fatalf("poll error after recovery = %q", a1.PollError)
	}
	expect(ws, "7d", 55, "2026-11-09T00:00:00Z", "polled")
	if h := f.headersFor[inference]; h["Authorization"] != "Bearer "+key || f.headersFor["https://opencode2.test/zen/go/v1/usage"]["Authorization"] != "Bearer "+key+"2" {
		t.Fatal("usage API not called with the account's key")
	}
}

type capturingObserver struct {
	windows map[string][]coreauth.QuotaWindowObservation
}

func (c *capturingObserver) ApplyQuotaObservation(authID, _ string, w []coreauth.QuotaWindowObservation) bool {
	if c.windows == nil {
		c.windows = map[string][]coreauth.QuotaWindowObservation{}
	}
	c.windows[authID] = w
	return true
}
