package requestlog

import (
	"context"
	"fmt"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/pricing"
)

func openPricedStore(t *testing.T) (*Store, string) {
	t.Helper()
	dsn := os.Getenv("TMX_REQUESTLOG_TEST_DSN")
	if dsn == "" {
		t.Skip("TMX_REQUESTLOG_TEST_DSN not set")
	}
	t.Setenv("TMX_PRICES", "off")
	t.Setenv("WRITABLE_PATH", t.TempDir())
	schema := fmt.Sprintf("tmx_test_%d", time.Now().UnixNano())
	store, err := Open(context.Background(), dsn, schema, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = store.db.Exec("DROP SCHEMA " + schema + " CASCADE")
		store.Close()
	})
	cat, err := pricing.ParseModelsDev([]byte(`{"anthropic":{"models":{"claude-opus-5-5":{"cost":{"input":4,"output":20,"cache_read":0.2,"cache_write":5}}}}}`))
	if err != nil {
		t.Fatal(err)
	}
	store.SetPriceCatalogForTest(cat, time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC))
	return store, schema
}

func costOf(t *testing.T, s *Store, where string) (*float64, string, string) {
	t.Helper()
	var cost *float64
	var as string
	var src *string
	if err := s.db.QueryRow(`SELECT cost_usd, priced_as, price_source FROM `+s.table()+` WHERE `+where).Scan(&cost, &as, &src); err != nil {
		t.Fatal(err)
	}
	ps := ""
	if src != nil {
		ps = *src
	}
	return cost, as, ps
}

// Rows are priced when written (incl. 1-hour cache writes), keep their price across an unchanged
// re-import, and are repriced when their tokens change; rows written without a catalog are priced
// by the pass.
func TestRowsPricedAtWrite(t *testing.T) {
	s, _ := openPricedStore(t)
	ctx := context.Background()
	w := &writer{s: s}
	w.write([]Row{{TS: time.Now(), TraceID: "live", Provider: "claude", Model: "claude-opus-5-5", Outcome: OutcomeSuccess,
		UncachedInput: 1_000_000, CacheWrite: 100_000, CacheWrite1h: 100_000, OutputNonReasoning: 1000}})
	cost, as, src := costOf(t, s, `trace_id = 'live'`)
	if want := 4 + 0.1*8 + 0.02; cost == nil || math.Abs(*cost-want) > 1e-9 || as != "anthropic/claude-opus-5-5" || src != "models.dev" {
		t.Fatalf("live row = %v %q %q", cost, as, src)
	}

	imp := Row{TS: time.Now(), Provider: "anthropic", Model: "claude-opus-5-5", Outcome: OutcomeSuccess,
		UncachedInput: 1_000_000, Source: "extern:claude-code", ImportRef: "claude-code:s#m1"}
	if err := s.UpsertImported(ctx, []Row{imp}); err != nil {
		t.Fatal(err)
	}
	// Prices change; an identical re-import keeps the original cost, a changed one is repriced.
	cat2, _ := pricing.ParseModelsDev([]byte(`{"anthropic":{"models":{"claude-opus-5-5":{"cost":{"input":10,"output":50}}}}}`))
	s.SetPriceCatalogForTest(cat2, time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC))
	if err := s.UpsertImported(ctx, []Row{imp}); err != nil {
		t.Fatal(err)
	}
	if cost, _, _ = costOf(t, s, `import_ref = 'claude-code:s#m1'`); cost == nil || math.Abs(*cost-4) > 1e-9 {
		t.Fatalf("unchanged re-import repriced: %v", cost)
	}
	imp.OutputNonReasoning = 100_000
	if err := s.UpsertImported(ctx, []Row{imp}); err != nil {
		t.Fatal(err)
	}
	if cost, _, _ = costOf(t, s, `import_ref = 'claude-code:s#m1'`); cost == nil || math.Abs(*cost-15) > 1e-9 {
		t.Fatalf("changed re-import = %v, want 15", cost)
	}

	// Pending and unpriced rows.
	if err := s.insert([]Row{{TS: time.Now(), TraceID: "pending", Provider: "anthropic", Model: "claude-opus-5-5", Outcome: OutcomeSuccess, UncachedInput: 1_000_000},
		{TS: time.Now(), TraceID: "unknown", Provider: "cursor", Model: "default", Outcome: OutcomeSuccess, UncachedInput: 5, TotalTokens: 5}}); err != nil {
		t.Fatal(err)
	}
	if n, err := s.PricePending(ctx, false); err != nil || n != 2 {
		t.Fatalf("priced %d (%v)", n, err)
	}
	if cost, _, _ = costOf(t, s, `trace_id = 'pending'`); cost == nil || math.Abs(*cost-10) > 1e-9 {
		t.Fatalf("pending row = %v", cost)
	}
	if cost, _, src = costOf(t, s, `trace_id = 'unknown'`); cost != nil || src != "unpriced" {
		t.Fatalf("unknown row = %v %q", cost, src)
	}
	if n, _ := s.PricePending(ctx, false); n != 0 {
		t.Fatalf("second pass priced %d", n)
	}
	sum, err := s.Summarize(ctx, Filter{From: time.Now().Add(-time.Hour), To: time.Now().Add(time.Hour)}, time.Hour)
	if err != nil || sum.Totals.Unpriced != 1 || math.Abs(sum.Totals.CostUSD-(4.82+15+10)) > 1e-9 || sum.Totals.CacheWrite1h != 100_000 {
		t.Fatalf("summary cost = %+v %v", sum.Totals.Cost, err)
	}
}

// A row the database refuses is dropped alone; the rest of its batch is written.
func TestWriterRejectsOnlyBadRows(t *testing.T) {
	s, _ := openPricedStore(t)
	w := &writer{s: s}
	w.write([]Row{
		{TS: time.Now(), TraceID: "ok1", Outcome: OutcomeSuccess},
		{TS: time.Now(), TraceID: "bad", Outcome: OutcomeSuccess, Error: "nul\x00byte"},
		{TS: time.Now(), TraceID: "ok2", Outcome: OutcomeSuccess},
	})
	var n int
	_ = s.db.QueryRow(`SELECT count(*) FROM ` + s.table()).Scan(&n)
	if n != 2 || s.rejected.Load() != 1 || len(w.pending) != 0 {
		t.Fatalf("rows=%d rejected=%d pending=%d", n, s.rejected.Load(), len(w.pending))
	}
}

// While the database is down rows wait (nothing is dropped) and are mirrored to the spool file;
// the next start writes them, and a retry of rows that did commit inserts nothing twice.
func TestWriterKeepsRowsWhileDatabaseIsDown(t *testing.T) {
	s, schema := openPricedStore(t)
	dsn := os.Getenv("TMX_REQUESTLOG_TEST_DSN")
	down, err := Open(context.Background(), dsn, schema, 0)
	if err != nil {
		t.Fatal(err)
	}
	_ = down.db.Close() // every insert and ping now fails
	w := &writer{s: down}
	w.write([]Row{{TS: time.Now(), TraceID: "kept", Outcome: OutcomeSuccess}})
	if len(w.pending) != 1 || down.dropped.Load() != 0 {
		t.Fatalf("pending=%d dropped=%d", len(w.pending), down.dropped.Load())
	}
	if raw, errR := os.ReadFile(down.spoolFile); errR != nil || !strings.Contains(string(raw), `"kept"`) {
		t.Fatalf("spool = %q %v", raw, errR)
	}
	// "Crash" and restart: the spool is read back and written, then removed.
	recovered := readSpool(down.spoolFile)
	w2 := &writer{s: s, pending: recovered, spooled: len(recovered) > 0}
	w2.write(nil)
	// The same rows again (as if the first insert had committed without an acknowledgement).
	w3 := &writer{s: s, pending: recovered}
	w3.write(nil)
	var n int
	_ = s.db.QueryRow(`SELECT count(*) FROM ` + s.table() + ` WHERE trace_id = 'kept'`).Scan(&n)
	if n != 1 {
		t.Fatalf("spooled row written %d times", n)
	}
	if _, errS := os.Stat(s.spoolFile); !os.IsNotExist(errS) {
		t.Fatalf("spool file left behind: %v", errS)
	}
}
