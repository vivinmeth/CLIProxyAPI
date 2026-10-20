package requestlog

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

func TestSessionTotalsAndSessionRollup(t *testing.T) {
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
	if err = store.insert([]Row{
		{TS: base, TraceID: "t1", SessionID: "big", Source: SourceProxy, Provider: "claude", Model: "opus", AuthID: "a", Machine: "macbook", APIKeyHash: "k1", Outcome: OutcomeSuccess, UncachedInput: 100, CacheRead: 900, OutputNonReasoning: 50, TotalTokens: 1050},
		{TS: base.Add(time.Minute), TraceID: "t2", SessionID: "big", Source: SourceProxy, Provider: "claude", Model: "haiku", AuthID: "b", Machine: "macbook", APIKeyHash: "k1", Outcome: OutcomeCanceled, TotalTokens: 10},
		{TS: base.Add(2 * time.Minute), TraceID: "t3", SessionID: "small", Source: SourceProxy, Provider: "claude", Model: "opus", AuthID: "a", Outcome: OutcomeSuccess, TotalTokens: 5},
		{TS: base.Add(3 * time.Minute), TraceID: "t4", Source: SourceProxy, Provider: "claude", Model: "opus", Outcome: OutcomeSuccess, TotalTokens: 999},
	}); err != nil {
		t.Fatal(err)
	}
	f := Filter{From: base.Add(-time.Minute), To: base.Add(time.Hour)}
	got, err := store.SessionTotals(ctx, f, "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].SessionID != "big" || got[1].SessionID != "small" {
		t.Fatalf("sessions = %+v (rows without a session id must be skipped)", got)
	}
	b := got[0]
	if b.Attempts != 2 || b.Success != 1 || b.Canceled != 1 || b.TotalTokens != 1060 || b.CacheRead != 900 ||
		len(b.Models) != 2 || len(b.AuthIDs) != 2 || len(b.Machines) != 1 || b.Machines[0] != "macbook" ||
		!b.FirstTS.Equal(base) {
		t.Fatalf("big = %+v", b)
	}
	if len(got[1].Machines) != 0 {
		t.Fatalf("empty machine should not be listed: %+v", got[1].Machines)
	}
	recent, err := store.SessionTotals(ctx, f, "recent", 1)
	if err != nil || len(recent) != 1 || recent[0].SessionID != "small" {
		t.Fatalf("recent = %+v %v", recent, err)
	}
	cube, _, err := store.Rollup(ctx, f, "day", "UTC", []string{"session_id", "model"}, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(cube) != 4 {
		t.Fatalf("rollup by session_id,model = %d cells, want 4", len(cube))
	}
}

func TestHarnessColumnsWrittenAndGrouped(t *testing.T) {
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
	if err = store.insert([]Row{
		{TS: base, TraceID: "a", SessionID: "s1", Source: SourceProxy, UserAgent: "claude-cli/2.1.304 (external, sdk-ts, agent-sdk/0.3.276)", Outcome: OutcomeSuccess, TotalTokens: 10},
		{TS: base, TraceID: "b", SessionID: "s2", Source: SourceProxy, UserAgent: "codex_exec/0.163.0 (Ubuntu)", Outcome: OutcomeSuccess, TotalTokens: 20},
		{TS: base, TraceID: "c", SessionID: "s3", Source: "extern:opencode", UserAgent: "opencode/1.2", Outcome: OutcomeSuccess, TotalTokens: 30},
	}); err != nil {
		t.Fatal(err)
	}
	// A row written before the columns existed is labelled by the backfill.
	if _, err = store.db.Exec(`INSERT INTO `+store.table()+` (ts, trace_id, source, user_agent, outcome, total_tokens)
		VALUES ($1, 'old', 'tmx-proxy', 'claude-cli/2.1.304 (external, cli)', 'success', 40)`, base); err != nil {
		t.Fatal(err)
	}
	store.backfillHarness()
	f := Filter{From: base.Add(-time.Minute), To: base.Add(time.Hour)}
	cube, _, err := store.Rollup(ctx, f, "day", "UTC", []string{"harness", "client_mode"}, 100)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]int64{}
	for _, c := range cube {
		got[c.Dims["harness"]+"/"+c.Dims["client_mode"]] = c.TotalTokens
	}
	want := map[string]int64{"claude-agent-sdk/sdk": 10, "codex/exec": 20, "opencode/": 30, "claude-code/cli": 40}
	if len(got) != len(want) {
		t.Fatalf("cube = %v", got)
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("cube = %v, want %v", got, want)
		}
	}
	f.Harness = "codex"
	sessions, err := store.SessionTotals(ctx, f, "", 10)
	if err != nil || len(sessions) != 1 || sessions[0].SessionID != "s2" || sessions[0].Harnesses[0] != "codex" {
		t.Fatalf("harness filter sessions = %+v %v", sessions, err)
	}
}
