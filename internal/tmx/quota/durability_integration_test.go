package quota

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/spool/spooltest"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

// Readings taken while Postgres is down are spooled, are not marked written meanwhile, and reach
// the table with their original ts once it is back.
func TestQuotaOutageLosesNothing(t *testing.T) {
	dsn := os.Getenv("TMX_REQUESTLOG_TEST_DSN")
	if dsn == "" {
		t.Skip("TMX_REQUESTLOG_TEST_DSN not set")
	}
	t.Setenv("TMX_SPOOL_DIR", t.TempDir())
	proxied, proxy := spooltest.DSN(t, dsn)
	db, err := sql.Open("pgx", proxied)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err = db.Exec(`DROP SCHEMA IF EXISTS tmxqd CASCADE`); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`CREATE SCHEMA tmxqd`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { proxy.Up(); _, _ = db.Exec(`DROP SCHEMA IF EXISTS tmxqd CASCADE`) })
	f := &fakeFetcher{auths: []*coreauth.Auth{}, calls: map[string]int{}}
	s, err := newService(context.Background(), db, "tmxqd", f)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.spool.Close)
	pct := func(v float64) *float64 { return &v }
	base := time.Date(2026, 10, 22, 4, 0, 0, 0, time.UTC)

	proxy.Down()
	ctx := context.Background()
	s.record(ctx, "a", []Window{{Window: "5h", UsedPct: pct(10)}}, "polled", base)
	s.record(ctx, "a", []Window{{Window: "5h", UsedPct: pct(20)}, {Window: "7d", UsedPct: pct(3)}}, "polled", base.Add(10*time.Minute))
	s.mu.Lock()
	_, marked := s.accounts["a"].written["5h"]
	s.mu.Unlock()
	if marked {
		t.Fatal("a reading was marked written while the database was down")
	}
	if d := s.Durability(); d.SpooledPending != 3 || d.Dropped != 0 {
		t.Fatalf("while down: %+v", d)
	}

	proxy.Up()
	deadline := time.Now().Add(30 * time.Second)
	for s.spool.Pending() > 0 {
		if time.Now().After(deadline) {
			t.Fatal("spooled snapshots not replayed")
		}
		time.Sleep(50 * time.Millisecond)
	}
	rows, err := db.Query(`SELECT ts, "window", used_pct FROM tmxqd.quota_snapshots ORDER BY ts, "window"`)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for rows.Next() {
		var ts time.Time
		var w string
		var used float64
		_ = rows.Scan(&ts, &w, &used)
		got = append(got, ts.UTC().Format("15:04")+" "+w)
	}
	_ = rows.Close()
	if len(got) != 3 || got[0] != "04:00 5h" || got[1] != "04:10 5h" || got[2] != "04:10 7d" {
		t.Fatalf("rows = %v", got)
	}
	s.mu.Lock()
	last := s.accounts["a"].written["5h"]
	s.mu.Unlock()
	if !last.writtAt.Equal(base.Add(10*time.Minute)) || *last.used != 20 {
		t.Fatalf("written after replay = %+v", last)
	}
	// Replaying a snapshot that already committed inserts nothing.
	s.mu.Lock()
	s.accounts["a"].written = map[string]written{}
	s.mu.Unlock()
	rec := snapshotRecord{Key: "fixed", TS: base.Add(time.Hour), AuthID: "a", Window: "5h", UsedPct: pct(30), Source: "polled"}
	for i := 0; i < 2; i++ {
		if err = s.insertSnapshot(ctx, rec); err != nil {
			t.Fatal(err)
		}
	}
	var n int
	_ = db.QueryRow(`SELECT count(*) FROM tmxqd.quota_snapshots WHERE snap_key = 'fixed'`).Scan(&n)
	if n != 1 {
		t.Fatalf("duplicate snapshot rows: %d", n)
	}
}
