package requestlog

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

// Runs against a real Postgres when TMX_REQUESTLOG_TEST_DSN is set (e.g. a throwaway container).
func TestStoreRoundTrip(t *testing.T) {
	dsn := os.Getenv("TMX_REQUESTLOG_TEST_DSN")
	if dsn == "" {
		t.Skip("TMX_REQUESTLOG_TEST_DSN not set")
	}
	ctx := context.Background()
	schema := fmt.Sprintf("tmx_test_%d", time.Now().UnixNano())
	store, err := Open(ctx, dsn, schema, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = store.db.Exec("DROP SCHEMA " + schema + " CASCADE")
		store.Close()
	})

	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Minute)
	rows := []Row{
		{TS: base, TraceID: "t1", SessionID: "s1", Provider: "claude", Model: "opus", AuthID: "a", AuthLabel: "a@x", Outcome: OutcomeSuccess, UncachedInput: 10, CacheRead: 90, OutputNonReasoning: 5, LatencyMs: 1000, TTFTMs: 200},
		{TS: base.Add(time.Minute), TraceID: "t2", SessionID: "s1", Provider: "claude", Model: "opus", AuthID: "b", AuthLabel: "b@x", Outcome: OutcomeCanceled, StatusCode: 499},
		{TS: base.Add(2 * time.Minute), TraceID: "t3", SessionID: "s2", Provider: "codex", Model: "gpt", AuthID: "c", AuthLabel: "c@x", Outcome: OutcomeFailed, StatusCode: 429, Error: "rate limited", UncachedInput: 60, CacheRead: 40},
		{TS: base.Add(3 * time.Minute), TraceID: "t3", SessionID: "s2", Provider: "codex", Model: "gpt", AuthID: "c", AuthLabel: "c@x", Outcome: OutcomeSuccess, UncachedInput: 40, CacheRead: 60, LatencyMs: 3000, TTFTMs: 400},
	}
	if err := store.insert(rows); err != nil {
		t.Fatal(err)
	}

	sum, err := store.Summarize(ctx, Filter{From: base.Add(-time.Minute), To: base.Add(10 * time.Minute)}, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	tot := sum.Totals
	if tot.Attempts != 4 || tot.Requests != 3 || tot.Success != 2 || tot.Failed != 1 || tot.Canceled != 1 {
		t.Fatalf("totals = %+v", tot)
	}
	// prompt = uncached + cache_read + cache_write = 300; hits = cache_read = 190
	if tot.PromptTokens != 300 || tot.CacheHitTokens != 190 {
		t.Fatalf("cache = prompt %d hits %d", tot.PromptTokens, tot.CacheHitTokens)
	}
	if sum.Sessions.Total != 2 || sum.Sessions.MultiAuth != 1 {
		t.Fatalf("sessions = %+v", sum.Sessions)
	}
	if len(sum.ByAuth) != 3 || len(sum.ByModel) != 2 || len(sum.Buckets) == 0 {
		t.Fatalf("groups auth=%d model=%d buckets=%d", len(sum.ByAuth), len(sum.ByModel), len(sum.Buckets))
	}

	items, err := store.List(ctx, Filter{From: base.Add(-time.Minute), To: base.Add(time.Hour), Outcome: OutcomeFailed}, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].Error != "rate limited" || items[0].StatusCode != 429 {
		t.Fatalf("failed items = %+v", items)
	}
	page, err := store.List(ctx, Filter{From: base.Add(-time.Minute), To: base.Add(time.Hour)}, 0, 2)
	if err != nil || len(page) != 2 {
		t.Fatalf("page = %d, %v", len(page), err)
	}
	rest, err := store.List(ctx, Filter{From: base.Add(-time.Minute), To: base.Add(time.Hour)}, page[1].ID, 0)
	if err != nil || len(rest) != 2 {
		t.Fatalf("rest = %d, %v", len(rest), err)
	}

	// The async writer path: Enqueue + Close flushes.
	store.Enqueue(Row{TS: base, Provider: "claude", Outcome: OutcomeSuccess})
	store.stopOnce.Do(func() { close(store.queue); <-store.done })
	if store.written.Load() != 1 {
		t.Fatalf("written = %d", store.written.Load())
	}
}

func TestCheckpointFlushesAcceptedRowsWithoutClosingDatabase(t *testing.T) {
	s, _ := openPricedStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, id := range []string{"checkpoint-before", "checkpoint-after"} {
		s.Enqueue(Row{TS: time.Now().UTC(), TraceID: id, Provider: "claude", Model: "opus", Outcome: OutcomeSuccess})
		if err := s.FlushContext(ctx); err != nil {
			t.Fatal(err)
		}
		var count int
		if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM `+s.table()+` WHERE trace_id=$1`, id).Scan(&count); err != nil || count != 1 {
			t.Fatalf("checkpoint row=%d err=%v", count, err)
		}
	}
}
