package quota

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

type fakeFetcher struct {
	mu      sync.Mutex
	auths   []*coreauth.Auth
	usage   string
	status  int
	calls   map[string]int
	headers map[string]string
	// codexUsage is served for the Codex usage URL; empty means 404.
	codexUsage string
}

func (f *fakeFetcher) TmxAuths() []*coreauth.Auth {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*coreauth.Auth, len(f.auths))
	for i, a := range f.auths {
		out[i] = a.Clone()
	}
	return out
}

func (f *fakeFetcher) TmxFetch(_ context.Context, _ *coreauth.Auth, url string, headers map[string]string) (int, []byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls[url]++
	f.headers = headers
	if url == codexUsageURL {
		if f.codexUsage == "" {
			return 404, nil, nil
		}
		return 200, []byte(f.codexUsage), nil
	}
	if url == claudeProfileURL {
		return 200, []byte(`{"account":{"has_claude_max":true},"organization":{"rate_limit_tier":"default_claude_max_20x"}}`), nil
	}
	return f.status, []byte(f.usage), nil
}

func usageJSON(fiveHour, sevenDay, fable float64) string {
	return fmt.Sprintf(`{"five_hour":{"utilization":%v,"resets_at":"2026-10-20T18:19:59Z"},`+
		`"seven_day":{"utilization":%v,"resets_at":"2026-10-27T07:59:59Z"},"limits":[{"kind":"weekly_scoped",`+
		`"percent":%v,"resets_at":"2026-10-27T07:59:59Z","scope":{"model":{"display_name":"Fable 5"}}}]}`,
		fiveHour, sevenDay, fable)
}

func openTestService(t *testing.T, f *fakeFetcher, clock *time.Time) *Service {
	t.Helper()
	dsn := os.Getenv("TMX_REQUESTLOG_TEST_DSN")
	if dsn == "" {
		t.Skip("TMX_REQUESTLOG_TEST_DSN not set")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err = db.Exec(`CREATE SCHEMA IF NOT EXISTS tmxq`); err != nil {
		t.Fatal(err)
	}
	s, err := newService(context.Background(), db, "tmxq", f)
	if err != nil {
		t.Fatal(err)
	}
	s.now = func() time.Time { return *clock }
	return s
}

func countRows(t *testing.T, s *Service) int {
	var n int
	if err := s.db.QueryRow(`SELECT count(*) FROM ` + s.table).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestQuotaServiceRecordsPassivePolledAndHistory(t *testing.T) {
	clock := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
	auth := &coreauth.Auth{ID: "claude-a.json", Provider: "claude", Label: "a@example.com"}
	auth.Quota.ObservedAt = clock.Add(-30 * time.Second)
	auth.Quota.Signals = map[string]string{
		"Anthropic-Ratelimit-Unified-5h-Utilization": "0.02",
		"Anthropic-Ratelimit-Unified-5h-Reset":       "1792520400",
		"Anthropic-Ratelimit-Unified-5h-Status":      "allowed",
	}
	idle := &coreauth.Auth{ID: "claude-idle.json", Provider: "claude"} // no traffic: poll only
	codex := &coreauth.Auth{ID: "codex-x.json", Provider: "codex"}
	f := &fakeFetcher{auths: []*coreauth.Auth{auth, idle, codex}, usage: usageJSON(2, 4, 10), status: 200,
		calls: map[string]int{}}
	s := openTestService(t, f, &clock)
	if _, err := s.db.Exec(`DELETE FROM ` + s.table); err != nil {
		t.Fatal(err)
	}
	s.accounts = map[string]*account{}
	ctx := context.Background()

	s.tick(ctx)
	// a: passive 5h, then the poll's 5h matches it (same value, reset within a minute) so only
	// 7d and 7d:fable-5 are new = 3 rows; idle: 3 polled rows. Codex polls but gets a 404 here.
	if n := countRows(t, s); n != 6 {
		t.Fatalf("rows after first tick = %d, want 6", n)
	}
	if f.calls[claudeUsageURL] != 2 || f.calls[claudeProfileURL] != 2 {
		t.Fatalf("calls = %v", f.calls)
	}
	if f.headers["Authorization"] != "Bearer $TOKEN$" {
		t.Fatalf("token placeholder not passed through: %v", f.headers)
	}

	// One minute later nothing changed and no poll is due: no rows.
	clock = clock.Add(time.Minute)
	s.tick(ctx)
	if n := countRows(t, s); n != 6 {
		t.Fatalf("rows after quiet tick = %d, want 6", n)
	}

	// Usage moves; the next poll (10 min) writes only the changed windows.
	f.usage = usageJSON(9, 4, 10)
	clock = clock.Add(10 * time.Minute)
	s.tick(ctx)
	if n := countRows(t, s); n != 8 {
		t.Fatalf("rows after change = %d, want 8 (5h for both accounts)", n)
	}

	// Restart right after: latest readings are restored, so an unchanged poll writes nothing.
	s2, err := newService(ctx, s.db, "tmxq", f)
	if err != nil {
		t.Fatal(err)
	}
	s2.now = s.now
	s2.tick(ctx)
	if n := countRows(t, s2); n != 8 {
		t.Fatalf("restart wrote %d duplicate rows", n-8)
	}
	if got := s2.Accounts()[0].Plan; got != "max_20x" {
		t.Fatalf("plan not restored: %q", got)
	}

	// 40 minutes of no change: heartbeat rows for every window once the 30-min mark passes.
	for i := 0; i < 4; i++ {
		clock = clock.Add(10 * time.Minute)
		s.tick(ctx)
	}
	if n := countRows(t, s); n <= 8 {
		t.Fatalf("no heartbeat rows written: %d", n)
	}

	// Rate limited: poll pauses for 30 minutes. Advance beyond all account jitter.
	f.status = 429
	clock = clock.Add(12 * time.Minute)
	s.tick(ctx)
	before := f.calls[claudeUsageURL]
	clock = clock.Add(10 * time.Minute)
	s.tick(ctx)
	if f.calls[claudeUsageURL] != before {
		t.Fatal("polled during rate-limit pause")
	}

	accounts := s.Accounts()
	if len(accounts) != 3 || accounts[0].Plan != "max_20x" || accounts[0].PollError == "" ||
		accounts[2].Provider != "codex" || accounts[2].PollError != "status 404" {
		t.Fatalf("accounts = %+v", accounts)
	}
	windows := map[string]Current{}
	for _, w := range accounts[0].Windows {
		windows[w.Window.Window] = w
	}
	if *windows["5h"].UsedPct != 9 || windows["7d:fable-5"].UsedPct == nil {
		t.Fatalf("current windows = %+v", windows)
	}

	hist, truncated, err := s.History(ctx, "claude-a.json", "5h", clock.Add(-24*time.Hour), clock.Add(time.Minute), 1000)
	if err != nil || truncated {
		t.Fatal(err, truncated)
	}
	if len(hist) < 3 || *hist[0].UsedPct != 2 || hist[0].Source != "passive" {
		t.Fatalf("history = %+v", hist)
	}

}

func TestQuotaPollNow(t *testing.T) {
	clock := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
	auth := &coreauth.Auth{ID: "claude-now.json", Provider: "claude"}
	codex := &coreauth.Auth{ID: "codex-now.json", Provider: "codex", Metadata: map[string]any{"account_id": "acct-1"}}
	apiKeyCodex := &coreauth.Auth{ID: "codex-key.json", Provider: "codex", Attributes: map[string]string{"api_key": "sk-x"}}
	f := &fakeFetcher{auths: []*coreauth.Auth{auth, codex, apiKeyCodex}, usage: usageJSON(7, 8, 9), status: 200, calls: map[string]int{},
		codexUsage: `{"plan_type":"prolite","rate_limit":{"primary_window":{"used_percent":12,"limit_window_seconds":604800,"reset_at":1793102375}}}`}
	s := openTestService(t, f, &clock)
	ctx := context.Background()
	acc, throttled, err := s.PollNow(ctx, "claude-now.json")
	if err != nil || throttled || len(acc.Windows) != 3 || acc.Plan != "max_20x" {
		t.Fatalf("poll now = %+v throttled=%v err=%v", acc, throttled, err)
	}
	clock = clock.Add(10 * time.Second)
	if _, throttled, _ = s.PollNow(ctx, "claude-now.json"); !throttled || f.calls[claudeUsageURL] != 1 {
		t.Fatalf("second poll within 30 s not throttled (calls %v)", f.calls)
	}
	cx, _, err := s.PollNow(ctx, "codex-now.json")
	if err != nil || cx.Plan != "prolite" || cx.Provider != "codex" || len(cx.Windows) != 1 ||
		cx.Windows[0].Window.Window != "7d" || *cx.Windows[0].UsedPct != 12 {
		t.Fatalf("codex poll = %+v %v", cx, err)
	}
	if f.headers["Chatgpt-Account-Id"] != "acct-1" {
		t.Fatalf("codex account header = %v", f.headers)
	}
	if _, _, err = s.PollNow(ctx, "codex-key.json"); !errors.Is(err, ErrUnsupportedProvider) {
		t.Fatalf("API-key codex err = %v", err)
	}
	if _, _, err = s.PollNow(ctx, "nope"); !errors.Is(err, ErrUnknownAccount) {
		t.Fatalf("unknown err = %v", err)
	}
}

// Fresh traffic headers slow background polling to the slow interval; a usage-API-only window
// passing its reset forces a poll; expired readings are flagged stale; poll-now stays live.
func TestQuotaSkipsPollWhenHeadersFresh(t *testing.T) {
	clock := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
	fiveHourReset := clock.Add(3 * time.Hour)
	auth := &coreauth.Auth{ID: "claude-busy.json", Provider: "claude"}
	setHeaders := func(observed time.Time) {
		auth.Quota.ObservedAt = observed
		auth.Quota.Signals = map[string]string{
			"Anthropic-Ratelimit-Unified-5h-Utilization": "0.10",
			"Anthropic-Ratelimit-Unified-5h-Reset":       strconv.FormatInt(fiveHourReset.Unix(), 10),
			"Anthropic-Ratelimit-Unified-7d-Utilization": "0.20",
			"Anthropic-Ratelimit-Unified-7d-Reset":       strconv.FormatInt(clock.Add(72*time.Hour).Unix(), 10),
		}
	}
	setHeaders(clock.Add(-time.Minute))
	// Fable resets 25 minutes in: once that passes, its value is stale and must be re-polled.
	fableReset := clock.Add(25 * time.Minute).Format(time.RFC3339)
	f := &fakeFetcher{auths: []*coreauth.Auth{auth}, status: 200, calls: map[string]int{},
		usage: `{"five_hour":{"utilization":10,"resets_at":"` + fiveHourReset.Format(time.RFC3339) + `"},` +
			`"limits":[{"kind":"weekly_scoped","percent":5,"resets_at":"` + fableReset + `","scope":{"model":{"display_name":"Fable"}}}]}`}
	s := openTestService(t, f, &clock)
	s.accounts = map[string]*account{}
	ctx := context.Background()

	s.tick(ctx) // first ever poll
	if f.calls[claudeUsageURL] != 1 {
		t.Fatalf("first tick calls = %v", f.calls)
	}
	// Traffic keeps the headers fresh: no background poll at 10 or 20 minutes.
	for _, m := range []int{10, 20} {
		clock = time.Date(2026, 10, 20, 12, m, 0, 0, time.UTC)
		setHeaders(clock.Add(-30 * time.Second))
		s.tick(ctx)
	}
	if f.calls[claudeUsageURL] != 1 {
		t.Fatalf("polled despite fresh headers: %v", f.calls)
	}
	acc := s.Accounts()[0]
	if !acc.PassiveFresh || acc.NextPollAt == nil || !acc.NextPollAt.Equal(time.Date(2026, 10, 20, 13, 0, 0, 0, time.UTC).Add(pollJitter(auth.ID, s.slowEvery))) {
		t.Fatalf("fresh account = %+v", acc)
	}
	// 12:26: Fable passed its reset. Its reading is flagged stale and the next tick re-polls it.
	clock = time.Date(2026, 10, 20, 12, 26, 0, 0, time.UTC)
	setHeaders(clock.Add(-30 * time.Second))
	for _, w := range s.Accounts()[0].Windows {
		if w.Window.Window == "7d:fable" && !w.Stale {
			t.Fatal("expired fable reading not flagged stale")
		}
	}
	fableReset = clock.Add(7 * 24 * time.Hour).Format(time.RFC3339)
	f.mu.Lock()
	f.usage = `{"five_hour":{"utilization":11,"resets_at":"` + fiveHourReset.Format(time.RFC3339) + `"},` +
		`"limits":[{"kind":"weekly_scoped","percent":0,"resets_at":"` + fableReset + `","scope":{"model":{"display_name":"Fable"}}}]}`
	f.mu.Unlock()
	s.tick(ctx)
	if f.calls[claudeUsageURL] != 2 {
		t.Fatalf("expired usage-only window did not force a poll: %v", f.calls)
	}
	// Traffic stops: headers age past the poll interval, normal 10-minute polling resumes.
	clock = time.Date(2026, 10, 20, 12, 45, 0, 0, time.UTC)
	s.tick(ctx)
	if f.calls[claudeUsageURL] != 3 || s.Accounts()[0].PassiveFresh {
		t.Fatalf("stale headers did not resume polling: %v", f.calls)
	}
	// Poll-now is live even right after a background poll... once the 30 s throttle passes.
	clock = clock.Add(time.Minute)
	setHeaders(clock.Add(-10 * time.Second))
	if _, throttled, err := s.PollNow(ctx, auth.ID); err != nil || throttled || f.calls[claudeUsageURL] != 4 {
		t.Fatalf("poll now with fresh headers: throttled=%v err=%v calls=%v", throttled, err, f.calls)
	}
}

// A removed or disabled login drops out of the current view; its history stays queryable.
func TestQuotaDropsRemovedAccounts(t *testing.T) {
	clock := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
	keep := &coreauth.Auth{ID: "claude-keep.json", Provider: "claude"}
	gone := &coreauth.Auth{ID: "claude-gone.json", Provider: "claude"}
	f := &fakeFetcher{auths: []*coreauth.Auth{keep, gone}, usage: usageJSON(1, 2, 3), status: 200, calls: map[string]int{}}
	s := openTestService(t, f, &clock)
	s.accounts = map[string]*account{}
	if _, err := s.db.Exec(`DELETE FROM ` + s.table); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	s.tick(ctx)
	if n := len(s.Accounts()); n != 2 {
		t.Fatalf("accounts = %d, want 2", n)
	}
	f.mu.Lock()
	f.auths = []*coreauth.Auth{keep}
	f.mu.Unlock()
	clock = clock.Add(time.Minute)
	s.tick(ctx)
	accs := s.Accounts()
	if len(accs) != 1 || accs[0].AuthID != "claude-keep.json" {
		t.Fatalf("after removal = %+v", accs)
	}
	hist, _, err := s.History(ctx, "claude-gone.json", "", clock.Add(-time.Hour), clock.Add(time.Hour), 100)
	if err != nil || len(hist) == 0 {
		t.Fatalf("history of removed account lost: %v %v", hist, err)
	}
	// Restarted service: accounts restored from history only appear once a tick sees them.
	s2, err := newService(ctx, s.db, "tmxq", f)
	if err != nil {
		t.Fatal(err)
	}
	s2.now = s.now
	s2.tick(ctx)
	if accs = s2.Accounts(); len(accs) != 1 || accs[0].AuthID != "claude-keep.json" {
		t.Fatalf("after restart = %+v", accs)
	}
}
