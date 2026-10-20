package quota

import (
	"context"
	"database/sql"
	"fmt"
	"os"
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
	// 7d and 7d:fable-5 are new = 3 rows; idle: 3 polled rows. Codex untouched.
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

	// Rate limited: poll pauses for 30 minutes.
	f.status = 429
	clock = clock.Add(10 * time.Minute)
	s.tick(ctx)
	before := f.calls[claudeUsageURL]
	clock = clock.Add(10 * time.Minute)
	s.tick(ctx)
	if f.calls[claudeUsageURL] != before {
		t.Fatal("polled during rate-limit pause")
	}

	accounts := s.Accounts()
	if len(accounts) != 2 || accounts[0].Plan != "max_20x" || accounts[0].PollError == "" {
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
