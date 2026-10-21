package archive

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/requestlog"
)

func usageMsg(id string, seq int64, ts time.Time, usage string) MessageIngest {
	m := MessageIngest{ExternalID: id, Seq: seq, Role: "assistant", TS: &ts, Model: "claude-opus-5-5",
		Message: json.RawMessage(`{"role":"assistant","content":"x"}`)}
	if usage != "" {
		m.Usage = json.RawMessage(usage)
	}
	return m
}

const u1 = `{"provider":"anthropic","model":"claude-opus-5-5","uncached_input":10,"cache_write":100,"cache_write_1h":60,"output":5}`

// Usage is written with the archive change; only turns from the session's first proxied call on
// are shadowed; a message whose usage moved loses its old row; messages without ids get keys of
// their own.
func TestImportedUsageRows(t *testing.T) {
	a, rl := openTestArchive(t)
	ctx := context.Background()
	t0 := time.Date(2026, 10, 20, 10, 0, 0, 0, time.UTC)
	// The proxy served this session from 10:30.
	if _, err := rl.DB().Exec(`INSERT INTO tmx.requests (ts, outcome, source, session_id) VALUES ($1, 'success', $2, 'mixed')`,
		t0.Add(30*time.Minute), requestlog.SourceProxy); err != nil {
		t.Fatal(err)
	}
	via := true
	in := SessionIngest{Harness: "claude-code", ExternalID: "mixed", ViaProxy: &via, Messages: []MessageIngest{
		usageMsg("direct", 1, t0.Add(10*time.Minute), u1),
		usageMsg("proxied", 2, t0.Add(40*time.Minute), u1),
		usageMsg("", 3, t0.Add(41*time.Minute), u1),
		usageMsg("", 4, t0.Add(42*time.Minute), u1),
	}}
	res, err := a.IngestSession(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	if res.UsageRows != 4 {
		t.Fatalf("usage rows = %d", res.UsageRows)
	}
	rows := map[string]bool{}
	r, err := rl.DB().Query(`SELECT import_ref, shadowed FROM tmx.requests WHERE import_ref IS NOT NULL`)
	if err != nil {
		t.Fatal(err)
	}
	for r.Next() {
		var ref string
		var sh bool
		_ = r.Scan(&ref, &sh)
		rows[ref] = sh
	}
	_ = r.Close()
	if len(rows) != 4 || rows["claude-code:mixed#direct"] || !rows["claude-code:mixed#proxied"] ||
		!rows["claude-code:mixed#seq-3"] || !rows["claude-code:mixed#seq-4"] {
		t.Fatalf("rows = %v", rows)
	}
	var w1h int64
	if err = rl.DB().QueryRow(`SELECT cache_write_1h FROM tmx.requests WHERE import_ref = 'claude-code:mixed#direct'`).Scan(&w1h); err != nil || w1h != 60 {
		t.Fatalf("cache_write_1h = %d %v", w1h, err)
	}

	// Re-import mid-response: the usage moved from "proxied" to a later chunk.
	in.Messages[1].Usage = nil
	in.Messages = append(in.Messages, usageMsg("later", 5, t0.Add(43*time.Minute), u1))
	if res, err = a.IngestSession(ctx, in); err != nil {
		t.Fatal(err)
	}
	var n int
	_ = rl.DB().QueryRow(`SELECT count(*) FROM tmx.requests WHERE import_ref = 'claude-code:mixed#proxied'`).Scan(&n)
	if n != 0 || res.UsageRemoved != 1 {
		t.Fatalf("stale row left: %d (removed %d)", n, res.UsageRemoved)
	}

	// Reshadow fixes rows written under the old whole-session rule.
	if _, err = rl.DB().Exec(`UPDATE tmx.requests SET shadowed = true WHERE import_ref IS NOT NULL`); err != nil {
		t.Fatal(err)
	}
	if fixed, errR := a.Reshadow(ctx); errR != nil || fixed != 1 {
		t.Fatalf("reshadowed %d (%v)", fixed, errR)
	}
}

// Archive and usage commit together: a usage write that fails rolls the ingest back.
func TestImportUsageSharesTheTransaction(t *testing.T) {
	a, rl := openTestArchive(t)
	ctx := context.Background()
	if !a.usageInArchiveDB(ctx, rl) {
		t.Fatal("test request log should share the archive database")
	}
	if _, err := rl.DB().Exec(`ALTER TABLE tmx.requests ADD CONSTRAINT tmp_no_big CHECK (uncached_input < 1000000)`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = rl.DB().Exec(`ALTER TABLE tmx.requests DROP CONSTRAINT IF EXISTS tmp_no_big`) })
	ts := time.Date(2026, 10, 20, 10, 0, 0, 0, time.UTC)
	in := SessionIngest{Harness: "claude-code", ExternalID: "atomic", Messages: []MessageIngest{
		usageMsg("m1", 1, ts, `{"provider":"anthropic","model":"claude-opus-5-5","uncached_input":5000000,"output":1}`)}}
	if _, err := a.IngestSession(ctx, in); err == nil {
		t.Fatal("ingest succeeded despite the usage write failing")
	}
	var n int
	_ = a.db.QueryRow(`SELECT count(*) FROM ` + schema + `.sessions WHERE id = 'claude-code:atomic'`).Scan(&n)
	if n != 0 {
		t.Fatal("archive committed without its usage rows")
	}
}

// A re-sent message whose only change is its usage counts as updated, not unchanged.
func TestResendWithChangedUsageCountsAsUpdated(t *testing.T) {
	a, _ := openTestArchive(t)
	ctx := context.Background()
	ts := time.Date(2026, 10, 20, 10, 0, 0, 0, time.UTC)
	in := SessionIngest{Harness: "claude-code", ExternalID: "resend", Messages: []MessageIngest{usageMsg("m1", 1, ts, u1)}}
	if _, err := a.IngestSession(ctx, in); err != nil {
		t.Fatal(err)
	}
	if res, err := a.IngestSession(ctx, in); err != nil || res.Unchanged != 1 || res.Updated != 0 {
		t.Fatalf("identical re-send = %+v %v", res, err)
	}
	in.Messages[0].Usage = json.RawMessage(`{"provider":"anthropic","model":"claude-opus-5-5","uncached_input":11,"output":5}`)
	if res, err := a.IngestSession(ctx, in); err != nil || res.Updated != 1 || res.Unchanged != 0 {
		t.Fatalf("usage-only change = %+v %v", res, err)
	}
}
