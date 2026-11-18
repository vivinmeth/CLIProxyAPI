package quota

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"
)

func TestQuotaStartsAtMigrationAndHistory(t *testing.T) {
	dsn := os.Getenv("TMX_REQUESTLOG_TEST_DSN")
	if dsn == "" {
		t.Skip("TMX_REQUESTLOG_TEST_DSN not set")
	}
	t.Setenv("TMX_SPOOL_DIR", t.TempDir())
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	const schema = "tmxqstarts"
	if _, err = db.Exec(`CREATE SCHEMA ` + schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = db.Exec(`DROP SCHEMA ` + schema + ` CASCADE`) })
	// The schema before starts_at, including a real pre-migration row.
	if _, err = db.Exec(`CREATE TABLE tmxqstarts.quota_snapshots (
		id BIGSERIAL PRIMARY KEY, ts TIMESTAMPTZ NOT NULL, auth_id TEXT NOT NULL,
		provider TEXT NOT NULL, label TEXT NOT NULL DEFAULT '', plan TEXT NOT NULL DEFAULT '',
		"window" TEXT NOT NULL, used_pct DOUBLE PRECISION, resets_at TIMESTAMPTZ,
		status TEXT NOT NULL DEFAULT '', source TEXT NOT NULL, snap_key TEXT
	)`); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	at := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	reset := at.Add(time.Hour)
	if _, err = db.Exec(`INSERT INTO tmxqstarts.quota_snapshots
		(ts, auth_id, provider, "window", used_pct, resets_at, source)
		VALUES ($1, 'a', 'codex', '5h', 12, $2, 'polled')`, at, reset); err != nil {
		t.Fatal(err)
	}
	var before, after uint32
	if err = db.QueryRow(`SELECT pg_relation_filenode('tmxqstarts.quota_snapshots')`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	f := &fakeFetcher{calls: map[string]int{}}
	s, err := newService(ctx, db, schema, f)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.spool.Close)
	if err = db.QueryRow(`SELECT pg_relation_filenode('tmxqstarts.quota_snapshots')`).Scan(&after); err != nil || before != after {
		t.Fatalf("migration rewrote table: before=%d after=%d err=%v", before, after, err)
	}
	var nullable string
	var defaultValue sql.NullString
	if err = db.QueryRow(`SELECT is_nullable, column_default FROM information_schema.columns
		WHERE table_schema = 'tmxqstarts' AND table_name = 'quota_snapshots' AND column_name = 'starts_at'`).Scan(&nullable, &defaultValue); err != nil || nullable != "YES" || defaultValue.Valid {
		t.Fatalf("column: nullable=%q default=%v err=%v", nullable, defaultValue, err)
	}
	if w := s.accounts["a"].current["5h"]; w.StartsAt != nil {
		t.Fatalf("old row restored with start: %v", w.StartsAt)
	}
	// A newly learned start must persist even when usage/reset did not change.
	start := reset.Add(-5 * time.Hour)
	w := Window{Window: "5h", UsedPct: pct(12), ResetsAt: &reset, StartsAt: &start}
	s.record(ctx, "a", []Window{w}, "polled", at.Add(time.Minute))
	s.record(ctx, "a", []Window{w}, "polled", at.Add(2*time.Minute)) // unchanged: no snapshot
	// Re-run startup to prove the migration is idempotent and starts survive restart.
	s2, err := newService(ctx, db, schema, f)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s2.spool.Close)
	if got := s2.accounts["a"].current["5h"].StartsAt; got == nil || !got.Equal(start) {
		t.Fatalf("restored start = %v, want %v", got, start)
	}
	s2.record(ctx, "a", []Window{w}, "polled", at.Add(3*time.Minute)) // unchanged after restart
	changedStart := start.Add(time.Second)
	w.StartsAt = &changedStart
	s2.record(ctx, "a", []Window{w}, "polled", at.Add(4*time.Minute))
	w.StartsAt = nil
	s2.record(ctx, "a", []Window{w}, "passive", at.Add(5*time.Minute))
	hist, truncated, err := s2.History(ctx, "a", "5h", at, at.Add(time.Hour), 100)
	if err != nil || truncated || len(hist) != 4 {
		t.Fatalf("history: %+v truncated=%v err=%v", hist, truncated, err)
	}
	for i, want := range []*time.Time{nil, &start, &changedStart, nil} {
		got := hist[i].StartsAt
		if (got == nil) != (want == nil) || (got != nil && !got.Equal(*want)) {
			t.Fatalf("row %d start = %v, want %v", i, got, want)
		}
	}
}
