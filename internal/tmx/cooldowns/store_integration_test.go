package cooldowns

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

type fakeManager struct {
	mu  sync.Mutex
	rep auth.CooldownReport
}

func (f *fakeManager) CooldownSnapshot() auth.CooldownReport {
	f.mu.Lock()
	defer f.mu.Unlock()
	r := f.rep
	r.At = time.Now()
	return r
}

func (f *fakeManager) set(seq uint64, ws ...auth.CooldownWindowState) {
	f.mu.Lock()
	f.rep = auth.CooldownReport{Seq: seq, Windows: ws}
	f.mu.Unlock()
}

func testDB(t *testing.T) (*sql.DB, string) {
	dsn := os.Getenv("TMX_COOLDOWNS_TEST_DSN")
	if dsn == "" {
		t.Skip("TMX_COOLDOWNS_TEST_DSN not set")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("tmx_cd_%d", time.Now().UnixNano())
	t.Cleanup(func() {
		_, _ = db.Exec("DROP SCHEMA IF EXISTS " + schema + " CASCADE")
		_, _ = db.Exec("DROP SCHEMA IF EXISTS " + schema + "_store CASCADE")
		_ = db.Close()
	})
	t.Setenv("TMX_COOLDOWNS_SPOOL", t.TempDir())
	return db, schema
}

// waitFor polls until cond holds (the tracker and writer are asynchronous).
func waitFor(t *testing.T, s *Store, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		_ = s.FlushContext(context.Background())
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s (stats %v)", what, s.Stats())
}

func countKind(t *testing.T, db *sql.DB, schema, kind string) int {
	var n int
	if err := db.QueryRow("SELECT count(*) FROM "+schema+".cooldown_events WHERE kind = $1", kind).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestStoreWindowsHistoryAndRestart(t *testing.T) {
	db, schema := testDB(t)
	ctx := context.Background()
	mgr := &fakeManager{}
	s, err := Open(ctx, db, schema, mgr)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	a := auth.CooldownWindowState{AuthID: "a", AuthIndex: "ia", AuthLabel: "a@x", Provider: "claude", Model: "opus",
		Reason: "quota", HTTPStatus: 429, RetryAt: now.Add(time.Hour), BackoffLevel: 1}
	b := auth.CooldownWindowState{AuthID: "b", Provider: "codex", Reason: "unauthorized", HTTPStatus: 401, RetryAt: now.Add(time.Hour)}

	s.ObserveCooldowns(auth.CooldownReport{Seq: 1, At: now, AuthID: "a", Windows: []auth.CooldownWindowState{a}})
	s.ObserveCooldowns(auth.CooldownReport{Seq: 2, At: now, AuthID: "b", Windows: []auth.CooldownWindowState{b}})
	a2 := a
	a2.RetryAt = now.Add(2 * time.Hour)
	s.ObserveCooldowns(auth.CooldownReport{Seq: 3, At: now.Add(time.Second), AuthID: "a", Windows: []auth.CooldownWindowState{a2}})
	s.ObserveCooldowns(auth.CooldownReport{Seq: 4, At: now.Add(time.Minute), AuthID: "b"})
	d := 30 * time.Second
	s.ObserveFailure(auth.FailureReport{At: now, AuthID: "a", Provider: "claude", Model: "opus", HTTPStatus: 429, RetryAfter: &d})
	s.ObserveFailure(auth.FailureReport{At: now, AuthID: "a", HTTPStatus: 500}) // not recorded
	waitFor(t, s, "5 events", func() bool { return s.Stats()["written"] >= 5 })
	mgr.set(4, a2)

	page, err := s.History(ctx, Query{From: now.Add(-time.Hour), To: now.Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != 5 {
		t.Fatalf("events: %+v", page.Events)
	}
	if len(page.Windows) != 2 {
		t.Fatalf("windows: %+v", page.Windows)
	}
	var wa, wb Window
	for _, w := range page.Windows {
		if w.AuthID == "a" {
			wa = w
		} else {
			wb = w
		}
	}
	if !wa.Open || wa.EndedAt != nil || wa.Updates != 1 || wa.RetryAt == nil || !wa.RetryAt.Equal(a2.RetryAt.Truncate(time.Microsecond)) ||
		wa.AuthIndex != "ia" || wa.Model != "opus" {
		t.Fatalf("window a: %+v", wa)
	}
	if wb.Open || wb.EndReason != EndCleared || wb.DurationMs != 60000 || wb.HTTPStatus != 401 {
		t.Fatalf("window b: %+v", wb)
	}

	// Paging + filters.
	p1, err := s.History(ctx, Query{From: now.Add(-time.Hour), To: now.Add(time.Hour), Limit: 2})
	if err != nil || len(p1.Events) != 2 || p1.NextCursor == "" {
		t.Fatalf("page 1: %v %+v", err, p1)
	}
	p2, err := s.History(ctx, Query{From: now.Add(-time.Hour), To: now.Add(time.Hour), Limit: 10, Cursor: p1.NextCursor})
	if err != nil || len(p2.Events) != 3 || p2.NextCursor != "" || len(p2.Windows) != 0 {
		t.Fatalf("page 2: %v %+v", err, p2)
	}
	onlyRL, err := s.History(ctx, Query{From: now.Add(-time.Hour), To: now.Add(time.Hour), AuthID: "a", Kinds: []string{KindRateLimited}})
	if err != nil || len(onlyRL.Events) != 1 || onlyRL.Events[0].RetryAfterMs != 30000 {
		t.Fatalf("kind filter: %v %+v", err, onlyRL.Events)
	}

	// Restart: a is still cooling after the restart, a second window for "c" was open in the
	// spool-less past and is gone now.
	cID := "orphan-c"
	if _, err = db.Exec(`INSERT INTO `+schema+`.cooldown_events (event_key, ts, kind, cooldown_id, auth_id, model, retry_at, started_at)
		VALUES ('k-c', $1, 'cooldown_start', $2, 'c', '', $3, $1)`, now, cID, now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	s.Close()
	mgr2 := &fakeManager{}
	mgr2.set(0, a2)
	s2, err := Open(ctx, db, schema, mgr2)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	waitFor(t, s2, "restart end", func() bool { return countKind(t, db, schema, KindEnd) == 2 })
	var reason string
	if err = db.QueryRow(`SELECT end_reason FROM `+schema+`.cooldown_events WHERE kind='cooldown_end' AND cooldown_id=$1`, cID).Scan(&reason); err != nil || reason != EndRestart {
		t.Fatalf("orphan end: %v %q", err, reason)
	}
	if n := countKind(t, db, schema, KindStart); n != 3 {
		t.Fatalf("a's window must continue across the restart, starts = %d", n)
	}
	if s2.Stats()["open_windows"] != 1 {
		t.Fatalf("stats: %v", s2.Stats())
	}
}

func TestStoreSpoolsWhileDatabaseIsDown(t *testing.T) {
	db, schema := testDB(t)
	ctx := context.Background()
	s, err := Open(ctx, db, schema, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`ALTER TABLE ` + schema + `.cooldown_events RENAME TO cooldown_events_away`); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	s.ObserveFailure(auth.FailureReport{At: now, AuthID: "a", HTTPStatus: 401})
	s.ObserveFailure(auth.FailureReport{At: now, AuthID: "a", HTTPStatus: 429, Message: "usage_limit_reached"})
	waitFor(t, s, "pending", func() bool { return s.Stats()["pending"] == 2 })
	s.Close()
	if _, err = os.Stat(filepath.Join(os.Getenv("TMX_COOLDOWNS_SPOOL"), spoolFileName)); err != nil {
		t.Fatalf("spool file: %v", err)
	}
	if s.Stats()["write_errors"] == 0 || s.Stats()["dropped"] != 0 {
		t.Fatalf("stats: %v", s.Stats())
	}
	if _, err = db.Exec(`ALTER TABLE ` + schema + `.cooldown_events_away RENAME TO cooldown_events`); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(ctx, db, schema, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	waitFor(t, s2, "spooled events written", func() bool {
		return countKind(t, db, schema, KindAuthError) == 1 && countKind(t, db, schema, KindUsageLimit) == 1
	})
	if _, err = os.Stat(filepath.Join(os.Getenv("TMX_COOLDOWNS_SPOOL"), spoolFileName)); !os.IsNotExist(err) {
		t.Fatalf("spool must be removed once written: %v", err)
	}
}

func TestCooldownStoreTrigger(t *testing.T) {
	db, schema := testDB(t)
	ctx := context.Background()
	s, err := Open(ctx, db, schema, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	storeTable := schema + "_store.cooldown_store"
	// Same DDL as internal/store/postgresstore.go.
	for _, stmt := range []string{"CREATE SCHEMA " + schema + "_store", `CREATE TABLE ` + storeTable + ` (
		auth_id TEXT NOT NULL, model TEXT NOT NULL DEFAULT '', content JSONB NOT NULL,
		deleted BOOLEAN NOT NULL DEFAULT FALSE, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), PRIMARY KEY (auth_id, model))`} {
		if _, err = db.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}
	s.installTrigger(ctx, storeTable)
	if s.Stats()["trigger_installed"] != 1 {
		t.Fatal("trigger not installed")
	}
	s.installTrigger(ctx, storeTable) // idempotent
	s.installTrigger(ctx, schema+"_store.missing")

	content := `{"auth_id":"a","model":"opus","provider":"claude","reason":"quota","next_retry_after":"2026-10-22T12:00:00Z",
		"quota":{"exceeded":true,"backoff_level":2,"next_recover_at":"2026-10-22T12:00:00Z"},"last_error":{"message":"slow","http_status":429}}`
	upsert := `INSERT INTO ` + storeTable + ` (auth_id, model, content, deleted) VALUES ('a','opus',$1,$2)
		ON CONFLICT (auth_id, model) DO UPDATE SET content = EXCLUDED.content, deleted = EXCLUDED.deleted`
	mustExec := func(q string, args ...any) {
		t.Helper()
		if _, errExec := db.Exec(q, args...); errExec != nil {
			t.Fatal(errExec)
		}
	}
	mustExec(upsert, content, false)
	mustExec(upsert, content, false) // unchanged: no row
	mustExec(upsert, `{}`, true)
	if n := countKind(t, db, schema, KindStoreSet); n != 1 {
		t.Fatalf("store_set rows: %d", n)
	}
	if n := countKind(t, db, schema, KindStoreClear); n != 1 {
		t.Fatalf("store_cleared rows: %d", n)
	}
	var status, backoff int
	var retry time.Time
	var source string
	if err = db.QueryRow(`SELECT http_status, backoff_level, retry_at, source FROM `+schema+`.cooldown_events WHERE kind='store_set'`).
		Scan(&status, &backoff, &retry, &source); err != nil || status != 429 || backoff != 2 || source != SourceTrigger ||
		!retry.Equal(time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)) {
		t.Fatalf("trigger row: %v %d %d %v %q", err, status, backoff, retry, source)
	}

	// The trigger must never fail the cooldown_store write, even when the history table is gone.
	mustExec(`ALTER TABLE ` + schema + `.cooldown_events RENAME TO cooldown_events_away`)
	mustExec(upsert, content, false)
	mustExec(`INSERT INTO `+storeTable+` (auth_id, model, content) VALUES ('b','',$1)`, `{"next_retry_after":"not a time"}`)
	mustExec(`ALTER TABLE ` + schema + `.cooldown_events_away RENAME TO cooldown_events`)
	// A malformed content row is also swallowed, not written half-way.
	mustExec(`INSERT INTO `+storeTable+` (auth_id, model, content) VALUES ('c','',$1)`, `{"next_retry_after":"garbage"}`)
	if n := countKind(t, db, schema, KindStoreSet); n != 1 {
		t.Fatalf("store_set rows after failures: %d", n)
	}
}
