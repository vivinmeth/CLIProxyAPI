package requestlog

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/spool/spooltest"
)

// openOutageStore opens a store whose database connection goes through a proxy the test can take
// down. TMX_REQUESTLOG_TEST_DSN must be a postgres:// URL.
func openOutageStore(t *testing.T, spoolDir string) (*Store, *spooltest.Proxy, string) {
	t.Helper()
	dsn := os.Getenv("TMX_REQUESTLOG_TEST_DSN")
	if dsn == "" {
		t.Skip("TMX_REQUESTLOG_TEST_DSN not set")
	}
	t.Setenv("TMX_PRICES", "off")
	t.Setenv("TMX_SPOOL_DIR", spoolDir)
	proxied, proxy := spooltest.DSN(t, dsn)
	schema := fmt.Sprintf("tmx_dur_%d", time.Now().UnixNano())
	s, err := Open(context.Background(), proxied, schema, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		proxy.Up()
		_, _ = s.db.Exec("DROP SCHEMA " + schema + " CASCADE")
		s.Close()
	})
	return s, proxy, schema
}

func countRows(t *testing.T, s *Store, where string) int {
	t.Helper()
	var n int
	if err := s.db.QueryRow(`SELECT count(*) FROM ` + s.table() + ` WHERE ` + where).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// A database outage loses nothing: rows taken while it is down are spooled on disk and reach the
// table, with their original timestamps, once it is back.
func TestOutageLosesNothing(t *testing.T) {
	s, proxy, _ := openOutageStore(t, t.TempDir())
	ctx := context.Background()
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)

	s.Enqueue(Row{TS: base, TraceID: "before", Outcome: OutcomeSuccess})
	if err := s.FlushContext(ctx); err != nil {
		t.Fatal(err)
	}
	proxy.Down()
	for i := 0; i < 500; i++ {
		s.Enqueue(Row{TS: base.Add(time.Duration(i) * time.Second), TraceID: fmt.Sprintf("down-%03d", i), Outcome: OutcomeSuccess})
	}
	if err := s.FlushContext(ctx); err != nil {
		t.Fatal(err)
	}
	d := s.Durability()
	if d.SpooledPending != 500 || d.Dropped != 0 || d.Queued != 0 {
		t.Fatalf("while down: %+v", d)
	}
	proxy.Up()
	waitUntil(t, "replay", func() bool { return s.spool.Pending() == 0 })
	if n := countRows(t, s, `trace_id LIKE 'down-%'`); n != 500 {
		t.Fatalf("rows after recovery = %d", n)
	}
	var first time.Time
	_ = s.db.QueryRow(`SELECT ts FROM ` + s.table() + ` WHERE trace_id = 'down-007'`).Scan(&first)
	if !first.Equal(base.Add(7 * time.Second)) {
		t.Fatalf("ts = %v, want %v", first, base.Add(7*time.Second))
	}
	d = s.Durability()
	if d.Replayed != 500 || d.Dropped != 0 || d.Deadlettered != 0 || d.SpooledBytes != 0 {
		t.Fatalf("after: %+v", d)
	}
}

// Rows spooled during an outage survive a restart (the process dies with the database down) and
// are written by the next start; replaying rows that already committed inserts nothing twice.
func TestSpooledRowsSurviveRestart(t *testing.T) {
	dir := t.TempDir()
	s, proxy, schema := openOutageStore(t, dir)
	proxy.Down()
	for i := 0; i < 20; i++ {
		s.Enqueue(Row{TS: time.Now(), TraceID: fmt.Sprintf("r-%02d", i), Outcome: OutcomeSuccess})
	}
	_ = s.FlushContext(context.Background())
	if s.spool.Pending() != 20 {
		t.Fatalf("spooled %d", s.spool.Pending())
	}
	// Restart: the spool is closed with the rows on disk; a new store on the same spool writes them.
	s.spool.Close()
	proxy.Up()
	s2, err := Open(context.Background(), os.Getenv("TMX_REQUESTLOG_TEST_DSN"), schema, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	waitUntil(t, "replay after restart", func() bool { return s2.spool.Pending() == 0 })
	if n := countRows(t, s2, `trace_id LIKE 'r-%'`); n != 20 {
		t.Fatalf("rows = %d", n)
	}
	// The same rows again (as if acknowledgements had been lost): no duplicates.
	var rows []Row
	r, _ := s2.db.Query(`SELECT row_key, trace_id, ts FROM ` + s2.table() + ` WHERE trace_id LIKE 'r-%'`)
	for r.Next() {
		var row Row
		_ = r.Scan(&row.RowKey, &row.TraceID, &row.TS)
		row.Outcome = OutcomeSuccess
		rows = append(rows, row)
	}
	_ = r.Close()
	s2.spoolRows(rows)
	waitUntil(t, "second replay", func() bool { return s2.spool.Pending() == 0 })
	if n := countRows(t, s2, `trace_id LIKE 'r-%'`); n != 20 {
		t.Fatalf("rows after a duplicate replay = %d", n)
	}
}

// A row the database refuses is dead-lettered (kept on disk, counted); the rest of its batch is
// written. The same holds for a refused row replayed from the spool.
func TestRefusedRowsAreDeadLettered(t *testing.T) {
	dir := t.TempDir()
	s, proxy, _ := openOutageStore(t, dir)
	s.write([]Row{
		{TS: time.Now(), TraceID: "ok1", Outcome: OutcomeSuccess},
		{TS: time.Now(), TraceID: "bad", Outcome: OutcomeSuccess, Error: "nul\x00byte"},
		{TS: time.Now(), TraceID: "ok2", Outcome: OutcomeSuccess},
	})
	if n := countRows(t, s, `true`); n != 2 || s.rejected.Load() != 1 {
		t.Fatalf("rows=%d rejected=%d", n, s.rejected.Load())
	}
	proxy.Down()
	s.write([]Row{{TS: time.Now(), TraceID: "bad2", Outcome: OutcomeSuccess, Error: "x\x00"},
		{TS: time.Now(), TraceID: "ok3", Outcome: OutcomeSuccess}})
	proxy.Up()
	waitUntil(t, "replay", func() bool { return s.spool.Pending() == 0 })
	d := s.Durability()
	if n := countRows(t, s, `true`); n != 3 || d.Deadlettered != 2 || d.DeadletterTotal != 2 || d.Dropped != 0 {
		t.Fatalf("rows=%d durability=%+v", n, d)
	}
	matches, _ := filepath.Glob(filepath.Join(s.spool.Dir(), "deadletter", "*.spool"))
	if len(matches) != 1 {
		t.Fatalf("dead-letter files %v", matches)
	}
	raw, _ := os.ReadFile(matches[0])
	if !strings.Contains(string(raw), `"bad"`) || !strings.Contains(string(raw), `"bad2"`) {
		t.Fatalf("dead letters = %q", raw)
	}
}

// Under overload (a tiny queue) rows overflow to the spool instead of being dropped.
func TestQueueOverflowSpools(t *testing.T) {
	old := queueSize
	queueSize = 1
	defer func() { queueSize = old }()
	s, _, _ := openOutageStore(t, t.TempDir())
	for i := 0; i < 2000; i++ {
		s.Enqueue(Row{TS: time.Now(), TraceID: fmt.Sprintf("o-%04d", i), Outcome: OutcomeSuccess})
	}
	_ = s.FlushContext(context.Background())
	waitUntil(t, "every row", func() bool { return countRows(t, s, `trace_id LIKE 'o-%'`) == 2000 })
	if d := s.Durability(); d.Dropped != 0 || d.Spooled == 0 {
		t.Fatalf("durability %+v", d)
	}
}

// A nonzero retention is ignored unless pruning is explicitly allowed.
func TestRetentionNeedsExplicitOverride(t *testing.T) {
	t.Setenv("TMX_REQUESTLOG_RETENTION_DAYS", "30")
	t.Setenv("TMX_REQUESTLOG_ALLOW_PRUNE", "")
	if got := retentionFromEnv(); got != 0 {
		t.Fatalf("retention without override = %d", got)
	}
	t.Setenv("TMX_REQUESTLOG_ALLOW_PRUNE", "yes")
	if got := retentionFromEnv(); got != 0 {
		t.Fatalf("retention with a sloppy override = %d", got)
	}
	t.Setenv("TMX_REQUESTLOG_ALLOW_PRUNE", allowPruneValue)
	if got := retentionFromEnv(); got != 30 {
		t.Fatalf("retention with override = %d", got)
	}
	t.Setenv("TMX_REQUESTLOG_RETENTION_DAYS", "-3")
	if got := retentionFromEnv(); got != 0 {
		t.Fatalf("negative retention = %d", got)
	}
}
