package resets

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

func TestBalanceSnapshotOf(t *testing.T) {
	st := Status{Eligible: true, Credits: []Credit{}}
	if err := ParseCodexCredits([]byte(codexCreditsBody), &st, testNow); err != nil {
		t.Fatal(err)
	}
	n := 0
	st.ApplicableCount = &n
	at := testNow
	b := balanceSnapshotOf(Account{AuthID: "codex-1", AuthIndex: "ix", Provider: "codex", CheckedAt: &at, Status: st}, SourcePoll, testNow.Add(time.Hour))
	if b.Credits == nil || *b.Credits != len(st.Credits) || b.ResetsLeft == nil || b.Usable == nil || !b.TS.Equal(testNow) {
		t.Fatalf("snapshot %+v", b)
	}
	if b.NextExpiresAt == nil || !b.NextExpiresAt.Equal(time.Date(2026, 10, 22, 20, 24, 0, 0, time.UTC)) {
		t.Fatalf("next expiry %v", b.NextExpiresAt)
	}
	failed := balanceSnapshotOf(Account{AuthID: "codex-1", Provider: "codex", Error: "rate_limited", Status: st}, SourcePoll, testNow)
	if failed.Credits != nil || failed.ResetsLeft != nil || failed.Usable != nil || len(failed.CreditList) != 0 {
		t.Fatalf("a failed read must store unknown counts, got %+v", failed)
	}
}

// Every provider read lands in reset_credit_snapshots with its source (poll / manual / redeem),
// failed reads with NULL counts; a failed write is kept and written with the next read.
func TestBalanceHistoryRecordsEveryRead(t *testing.T) {
	dsn := os.Getenv("TMX_REQUESTLOG_TEST_DSN")
	if dsn == "" {
		t.Skip("TMX_REQUESTLOG_TEST_DSN not set")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("tmxbal_%d", time.Now().UnixNano())
	if _, err = db.Exec(`CREATE SCHEMA ` + schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = db.Exec(`DROP SCHEMA ` + schema + ` CASCADE`)
		_ = db.Close()
	})
	ctx := context.Background()
	j, err := newPGJournal(ctx, db, schema)
	if err != nil {
		t.Fatal(err)
	}
	balances, err := newBalanceLog(ctx, db, schema)
	if err != nil {
		t.Fatal(err)
	}

	fail := false
	used := 80.0
	f := &fakeBackend{auths: []*coreauth.Auth{codexAuth()}}
	f.handle = func(_, url string, _ []byte) (int, string, error) {
		switch {
		case fail:
			return 429, "", nil
		case strings.HasSuffix(url, "/wham/usage"):
			return 200, `{"rate_limit": {"primary_window": {"used_percent": ` + jsonNum(used) + `, "limit_window_seconds": 604800, "reset_at": 1793102375}}, "rate_limit_reset_credits": {"applicable_available_count": 1}}`, nil
		case strings.HasSuffix(url, "/rate-limit-reset-credits"):
			return 200, codexCreditsBody, nil
		case strings.HasSuffix(url, "/consume"):
			used = 0
			return 200, `{"code": "reset"}`, nil
		}
		return 500, "", nil
	}
	clock := testNow
	s := New(f, j)
	s.balances = balances
	s.now = func() time.Time { return clock }

	s.Accounts(ctx, false) // poll
	clock = clock.Add(time.Minute)
	if _, _, err = s.PollNow(ctx, "codex-1"); err != nil { // manual
		t.Fatal(err)
	}
	clock = clock.Add(time.Minute)
	if _, err = s.Redeem(ctx, "codex-1", "c1", "test"); err != nil { // redeem: before + after
		t.Fatal(err)
	}

	// The table is unavailable for one read: that row waits and is written with the next one.
	if _, err = db.Exec(`ALTER TABLE ` + schema + `.reset_credit_snapshots RENAME TO rcs_off`); err != nil {
		t.Fatal(err)
	}
	fail = true
	clock = clock.Add(cacheTTL)
	s.Accounts(ctx, false) // failed read, failed write
	if st := s.BalanceStats(); st.Pending != 1 || st.WriteFailures != 1 {
		t.Fatalf("stats after failed write %+v", st)
	}
	if _, err = db.Exec(`ALTER TABLE ` + schema + `.rcs_off RENAME TO reset_credit_snapshots`); err != nil {
		t.Fatal(err)
	}
	clock = clock.Add(time.Minute)
	if _, _, err = s.PollNow(ctx, "codex-1"); err != nil && err != ErrReadFailed {
		t.Fatal(err)
	}
	if st := s.BalanceStats(); st.Pending != 0 || st.Rows != 6 {
		t.Fatalf("stats after retry %+v", st)
	}

	from, to := testNow.Add(-time.Hour), testNow.Add(time.Hour)
	rows, next, err := s.BalanceHistory(ctx, BalanceQuery{From: from, To: to})
	if err != nil || next != 0 {
		t.Fatalf("history: %v next=%d", err, next)
	}
	var sources []string
	for _, r := range rows {
		sources = append(sources, r.Source)
	}
	if got := strings.Join(sources, ","); got != "poll,manual,redeem,redeem,poll,manual" {
		t.Fatalf("sources %s", got)
	}
	first := rows[0]
	if first.AuthID != "codex-1" || first.AuthIndex == "" || first.Provider != "codex" || first.Credits == nil ||
		*first.Credits != 2 || first.Usable == nil || first.NextExpiresAt == nil || len(first.CreditList) != 2 ||
		first.ApplicableCount == nil || *first.ApplicableCount != 1 || !first.TS.Equal(testNow) {
		t.Fatalf("first row %+v", first)
	}
	if failed := rows[4]; failed.Error != "rate_limited" || failed.Credits != nil || failed.ResetsLeft != nil {
		t.Fatalf("failed read row %+v", failed)
	}

	page, next, err := s.BalanceHistory(ctx, BalanceQuery{From: from, To: to, Limit: 4})
	if err != nil || len(page) != 4 || next != page[3].ID {
		t.Fatalf("page 1: n=%d next=%d err=%v", len(page), next, err)
	}
	page, next, err = s.BalanceHistory(ctx, BalanceQuery{From: from, To: to, Limit: 4, AfterID: next})
	if err != nil || len(page) != 2 || next != 0 {
		t.Fatalf("page 2: n=%d next=%d err=%v", len(page), next, err)
	}
	redeems, _, err := s.BalanceHistory(ctx, BalanceQuery{From: from, To: to, Source: SourceRedeem, AuthID: "codex-1", Provider: "codex"})
	if err != nil || len(redeems) != 2 {
		t.Fatalf("source filter: n=%d err=%v", len(redeems), err)
	}
	if other, _, _ := s.BalanceHistory(ctx, BalanceQuery{From: from, To: to, AuthID: "nobody"}); len(other) != 0 {
		t.Fatalf("auth filter returned %d", len(other))
	}
}

func TestPollEveryEnv(t *testing.T) {
	for raw, want := range map[string]time.Duration{"": time.Hour, "15": 15 * time.Minute, "0": 0, "off": 0, "x": time.Hour} {
		t.Setenv("TMX_RESETS_POLL_MINUTES", raw)
		if got := pollEvery(); got != want {
			t.Fatalf("%q: %s want %s", raw, got, want)
		}
	}
}
