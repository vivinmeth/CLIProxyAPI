package requestlog

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
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
	t.Setenv("TMX_SPOOL_DIR", t.TempDir())
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
	s.write([]Row{{TS: time.Now(), TraceID: "live", Provider: "claude", Model: "claude-opus-5-5", Outcome: OutcomeSuccess,
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

// An estimate (muse-wonder-full-1.0) prices new rows and is counted as estimated; rows already
// stored as unpriced stay unpriced, through the retry pass and a re-import.
func TestEstimatesPriceNewRowsOnly(t *testing.T) {
	s, _ := openPricedStore(t)
	ctx := context.Background()
	old := Row{TS: time.Now(), Provider: "meta", Model: "muse-wonder-full-1.0", Outcome: OutcomeSuccess, UncachedInput: 1_000_000,
		TotalTokens: 1_000_000, Source: "extern:muse-code", ImportRef: "muse:s#old"}
	src := "unpriced"
	old.PriceSource = &src
	if err := s.insert([]Row{old, {TS: time.Now(), TraceID: "old-live", Provider: "meta", Model: "muse-wonder-full-1.0",
		Outcome: OutcomeSuccess, UncachedInput: 1_000_000, TotalTokens: 1_000_000, PriceSource: &src}}); err != nil {
		t.Fatal(err)
	}
	if n, err := s.PricePending(ctx, true); err != nil || n != 0 {
		t.Fatalf("retry priced %d (%v)", n, err)
	}
	old.PriceSource = nil
	if err := s.UpsertImported(ctx, []Row{old}); err != nil {
		t.Fatal(err)
	}
	for _, where := range []string{`trace_id = 'old-live'`, `import_ref = 'muse:s#old'`} {
		if cost, _, ps := costOf(t, s, where); cost != nil || ps != "unpriced" {
			t.Fatalf("%s repriced: %v %q", where, cost, ps)
		}
	}
	s.write([]Row{{TS: time.Now(), TraceID: "new", Provider: "meta", Model: "muse-wonder-full-1.0", Outcome: OutcomeSuccess,
		UncachedInput: 1_000_000, CacheRead: 1_000_000, OutputNonReasoning: 100_000, TotalTokens: 2_100_000}})
	if cost, as, ps := costOf(t, s, `trace_id = 'new'`); cost == nil || math.Abs(*cost-(10+0.25+5)) > 1e-9 ||
		as != "meta/muse-wonder-full-1.0" || ps != "estimate:fable-5.1" {
		t.Fatalf("new row = %v %q %q", cost, as, ps)
	}
	f := Filter{From: time.Now().Add(-time.Hour), To: time.Now().Add(time.Hour)}
	sum, err := s.Summarize(ctx, f, time.Hour)
	if err != nil || sum.Totals.Estimated != 1 || sum.Totals.Unpriced != 2 || math.Abs(sum.Totals.CostEstimatedUSD-15.25) > 1e-9 {
		t.Fatalf("summary = %+v %v", sum.Totals.Cost, err)
	}
	cells, _, err := s.Rollup(ctx, f, "day", "UTC", []string{"model"}, 100)
	if err != nil || len(cells) != 1 || cells[0].Estimated != 1 || cells[0].Unpriced != 2 {
		t.Fatalf("rollup = %+v %v", cells, err)
	}
}

// Catalog snapshots are never deleted; an unchanged fetch isn't stored again and keeps naming the
// stored snapshot; the latest stored one is what a restart loads.
func TestPriceCatalogsKeptAndDeduped(t *testing.T) {
	s, schema := openPricedStore(t)
	ctx := context.Background()
	body := `{"anthropic":{"models":{"claude-opus-5-5":{"cost":{"input":4,"output":20}}}}}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(body)) }))
	t.Cleanup(srv.Close)
	t.Setenv("TMX_PRICES_CATALOG_URL", srv.URL)
	count := func() int {
		var n int
		if err := s.db.QueryRow(`SELECT count(*) FROM ` + schema + `.price_catalogs`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	first, err := s.fetchSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	again, err := s.fetchSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n := count(); n != 1 || !again.fetchedAt.Equal(first.fetchedAt) || !again.checkedAt.After(first.fetchedAt) {
		t.Fatalf("identical fetch: rows=%d fetched %s vs %s checked %s", n, again.fetchedAt, first.fetchedAt, again.checkedAt)
	}
	// More distinct catalogs than the old 30-snapshot cap: every one is kept.
	var last *priceSnapshot
	for i := 0; i < 32; i++ {
		body = fmt.Sprintf(`{"anthropic":{"models":{"claude-opus-5-5":{"cost":{"input":%d,"output":20}}}}}`, 5+i)
		if last, err = s.fetchSnapshot(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if n := count(); n != 33 {
		t.Fatalf("rows=%d, want 33 (nothing deleted)", n)
	}
	loaded, err := s.loadSnapshot(ctx)
	if err != nil || loaded == nil || !loaded.fetchedAt.Equal(last.fetchedAt) {
		t.Fatalf("loadSnapshot = %+v, %v; want fetched_at %s", loaded, err, last.fetchedAt)
	}
	c, ok := loaded.cat.Price(pricing.Usage{Provider: "claude", Model: "claude-opus-5-5", Uncached: 1_000_000})
	if !ok || math.Abs(c.Total-36) > 1e-9 {
		t.Fatalf("latest catalog price = %+v %v", c, ok)
	}
}

// A stored snapshot from before a code-pinned price is loaded with it, stored as a new snapshot.
func TestLoadSnapshotAddsPinnedPrices(t *testing.T) {
	s, schema := openPricedStore(t)
	ctx := context.Background()
	if _, err := s.db.Exec(`INSERT INTO `+schema+`.price_catalogs (fetched_at, source, catalog) VALUES ($1, 'models.dev', $2)`,
		time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC), `{"anthropic":{"claude-opus-5-5":{"input":4,"output":20,"cache_read":0.2,"cache_write":5}}}`); err != nil {
		t.Fatal(err)
	}
	snap, err := s.loadSnapshot(ctx)
	if err != nil || snap == nil {
		t.Fatalf("load: %v", err)
	}
	if m, ok := snap.cat["meta"]["muse-wonder-full-1.0"]; !ok || !pricing.IsEstimate(m.Source) || !snap.fetchedAt.After(time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("pinned estimate missing: %+v at %s", m, snap.fetchedAt)
	}
	again, err := s.loadSnapshot(ctx)
	if err != nil || !again.fetchedAt.Equal(snap.fetchedAt) {
		t.Fatalf("second load stored another snapshot: %v %s vs %s", err, again.fetchedAt, snap.fetchedAt)
	}
}
