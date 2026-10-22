package pool

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

func openHistoryDB(t *testing.T) (*sql.DB, string) {
	t.Helper()
	dsn := os.Getenv("TMX_REQUESTLOG_TEST_DSN")
	if dsn == "" {
		t.Skip("TMX_REQUESTLOG_TEST_DSN not set")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("tmxpool_%d", time.Now().UnixNano())
	if _, err = db.Exec(`CREATE SCHEMA ` + schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = db.Exec(`DROP SCHEMA ` + schema + ` CASCADE`)
		_ = db.Close()
	})
	return db, schema
}

// Each tick stores one row per credential under one shared snapshot_ts, with the masked ids the
// pool endpoint shows; history pages oldest first and filters by auth_id; a failed write is kept
// and written on the next tick.
func TestPoolRecorderStoresSnapshotsAndPages(t *testing.T) {
	db, schema := openHistoryDB(t)
	ctx := context.Background()
	now := time.Now()
	creds := []*auth.Auth{
		{ID: "claude-alice@example.com.json", Provider: "claude", Status: auth.StatusActive,
			Metadata: map[string]any{"access_token": "secret-token", "plan_type": "max_20x"},
			Quota: auth.QuotaState{ObservedAt: now, Signals: map[string]string{
				"Anthropic-Ratelimit-Unified-5h-Utilization": "0.25",
				"Anthropic-Ratelimit-Unified-5h-Reset":       strconv.FormatInt(now.Add(time.Hour).Unix(), 10),
				"Anthropic-Ratelimit-Unified-7d-Utilization": "0.5",
				"Anthropic-Ratelimit-Unified-7d-Reset":       strconv.FormatInt(now.Add(48*time.Hour).Unix(), 10),
			}}},
		{ID: "codex-bob@example.com.json", Provider: "codex", Status: auth.StatusError, Unavailable: true,
			NextRetryAfter: now.Add(time.Hour), Quota: auth.QuotaState{Exceeded: true, NextRecoverAt: now.Add(time.Hour)}},
	}
	build := func(ctx context.Context) Snapshot {
		return Build(ctx, creds, map[string]int{"claude-alice@example.com.json": 3}, nil, nil, "")
	}
	rec, err := NewRecorder(ctx, db, schema, build, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err = rec.Tick(ctx); err != nil {
		t.Fatal(err)
	}

	// A failed write stays queued and lands with the next snapshot.
	if _, err = db.Exec(`ALTER TABLE ` + schema + `.pool_snapshots RENAME TO pool_snapshots_off`); err != nil {
		t.Fatal(err)
	}
	if err = rec.Tick(ctx); err == nil {
		t.Fatal("write to a missing table succeeded")
	}
	if st := rec.Stats(); st.Pending != 1 || st.WriteFailures != 1 {
		t.Fatalf("stats after failure = %+v", st)
	}
	if _, err = db.Exec(`ALTER TABLE ` + schema + `.pool_snapshots_off RENAME TO pool_snapshots`); err != nil {
		t.Fatal(err)
	}
	if err = rec.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if err = rec.CloseContext(ctx); err != nil {
		t.Fatal(err)
	}
	st := rec.Stats()
	if st.Snapshots != 3 || st.Rows != 6 || st.Pending != 0 || st.LastSnapshotAt == nil {
		t.Fatalf("stats = %+v", st)
	}

	var raw string
	if err = db.QueryRow(`SELECT string_agg(row_to_json(p)::text, ',') FROM ` + schema + `.pool_snapshots p`).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"alice@example.com", "bob@example.com", "secret-token", "access_token"} {
		if strings.Contains(raw, forbidden) {
			t.Fatalf("stored %q", forbidden)
		}
	}
	var distinct int
	if err = db.QueryRow(`SELECT count(DISTINCT snapshot_ts) FROM ` + schema + `.pool_snapshots`).Scan(&distinct); err != nil {
		t.Fatal(err)
	}
	if distinct != 3 {
		t.Fatalf("distinct snapshot_ts = %d", distinct)
	}

	from, to := now.Add(-time.Hour), now.Add(time.Hour)
	page, next, err := rec.History(ctx, HistoryQuery{From: from, To: to, Limit: 4})
	if err != nil {
		t.Fatal(err)
	}
	if len(page) != 4 || next != page[3].ID || page[0].SnapshotTS != page[1].SnapshotTS {
		t.Fatalf("page 1: n=%d next=%d", len(page), next)
	}
	rest, next2, err := rec.History(ctx, HistoryQuery{From: from, To: to, AfterID: next, Limit: 4})
	if err != nil || len(rest) != 2 || next2 != 0 {
		t.Fatalf("page 2: n=%d next=%d err=%v", len(rest), next2, err)
	}
	a := page[0]
	if a.Provider != "claude" || a.BoundSessions != 3 || a.PlanType != "max_20x" || a.PickRank == nil ||
		a.Used5hPct == nil || *a.Used5hPct != 25 || a.Used7dPct == nil || *a.Used7dPct != 50 ||
		a.Resets7dAt == nil || len(a.Windows) != 2 || a.AuthIndex == "" {
		b, _ := json.Marshal(a)
		t.Fatalf("claude row = %s", b)
	}
	b := page[1]
	if b.Reason != "quota" || !b.Unavailable || b.NextRetryAfter == nil || b.CooldownCount != len(b.Cooldowns) || b.CooldownCount == 0 || b.PickRank != nil {
		raw, _ := json.Marshal(b)
		t.Fatalf("codex row = %s", raw)
	}

	only, _, err := rec.History(ctx, HistoryQuery{From: from, To: to, AuthID: b.AuthID})
	if err != nil || len(only) != 3 {
		t.Fatalf("auth filter: n=%d err=%v", len(only), err)
	}
	for _, r := range only {
		if r.AuthID != b.AuthID {
			t.Fatalf("auth filter returned %s", r.AuthID)
		}
	}
	none, _, err := rec.History(ctx, HistoryQuery{From: to, To: to.Add(time.Hour)})
	if err != nil || len(none) != 0 {
		t.Fatalf("out of range: n=%d err=%v", len(none), err)
	}
}

func TestSnapshotEveryEnv(t *testing.T) {
	for raw, want := range map[string]time.Duration{"": 5 * time.Minute, "15": 15 * time.Minute, "0": 0, "off": 0, "x": 5 * time.Minute} {
		t.Setenv("TMX_POOL_SNAPSHOT_MINUTES", raw)
		if got := SnapshotEvery(); got != want {
			t.Fatalf("%q: %s want %s", raw, got, want)
		}
	}
}
