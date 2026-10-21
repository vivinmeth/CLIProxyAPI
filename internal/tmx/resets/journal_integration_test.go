package resets

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// TestPGJournalCarriesClearsAndMigratesLegacyTable needs TMX_REQUESTLOG_TEST_DSN (throwaway
// postgres): a journal created before the clears column gains it, legacy rows read as unknown
// scope (nil), and new claims round-trip their scope.
func TestPGJournalCarriesClearsAndMigratesLegacyTable(t *testing.T) {
	dsn := os.Getenv("TMX_REQUESTLOG_TEST_DSN")
	if dsn == "" {
		t.Skip("TMX_REQUESTLOG_TEST_DSN not set")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()
	for _, stmt := range []string{
		`DROP SCHEMA IF EXISTS tmxresets CASCADE`,
		`CREATE SCHEMA tmxresets`,
		// The table as journal.go created it before the clears column.
		`CREATE TABLE tmxresets.reset_redemptions (
			id BIGSERIAL PRIMARY KEY, created_at TIMESTAMPTZ NOT NULL, settled_at TIMESTAMPTZ,
			auth_id TEXT NOT NULL, provider TEXT NOT NULL, label TEXT NOT NULL DEFAULT '',
			credit_id TEXT NOT NULL, credit_title TEXT NOT NULL DEFAULT '', request_id TEXT NOT NULL,
			state TEXT NOT NULL, outcome TEXT NOT NULL DEFAULT '', confirmed BOOLEAN NOT NULL DEFAULT FALSE,
			warning TEXT NOT NULL DEFAULT '', before JSONB, after JSONB, actor TEXT NOT NULL DEFAULT '')`,
		`INSERT INTO tmxresets.reset_redemptions (created_at, auth_id, provider, credit_id, request_id, state)
			VALUES (now(), 'claude-1', 'claude', 'legacy', 'req-legacy', 'pending')`,
	} {
		if _, err = db.ExecContext(ctx, stmt); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { _, _ = db.Exec(`DROP SCHEMA IF EXISTS tmxresets CASCADE`) })

	j, err := newPGJournal(ctx, db, "tmxresets")
	if err != nil {
		t.Fatalf("migrating a legacy journal: %v", err)
	}
	since := time.Now().Add(-time.Hour)
	legacy, err := j.pending(ctx, "claude-1", "legacy", since)
	if err != nil || legacy == nil {
		t.Fatalf("legacy claim: %v %v", legacy, err)
	}
	if legacy.Clears != nil {
		t.Fatalf("a legacy claim has unknown scope, got %v", legacy.Clears)
	}

	r := &Redemption{CreatedAt: time.Now().UTC(), AuthID: "claude-1", Provider: "claude", CreditID: "g1",
		RequestID: "req-1", State: "pending", Clears: []string{"5h", "7d_oi"}}
	if err = j.insert(ctx, r); err != nil {
		t.Fatal(err)
	}
	got, err := j.pending(ctx, "claude-1", "g1", since)
	if err != nil || got == nil || strings.Join(got.Clears, ",") != "5h,7d_oi" {
		t.Fatalf("scope round-trip: %+v %v", got, err)
	}
	// Creating the journal again (every start) is a no-op.
	if _, err = newPGJournal(ctx, db, "tmxresets"); err != nil {
		t.Fatal(err)
	}
}
