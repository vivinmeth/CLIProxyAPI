package archive

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/capture"
)

// Needs TMX_REQUESTLOG_TEST_DSN (archive + request log) and TMX_ANALYTICS_TEST_DSN (a second,
// throwaway database for the analytics side).
func openTestAnalytics(t *testing.T) (*Archive, *analytics) {
	t.Helper()
	dsn := os.Getenv("TMX_ANALYTICS_TEST_DSN")
	if dsn == "" {
		t.Skip("TMX_ANALYTICS_TEST_DSN not set")
	}
	a, _ := openTestArchive(t)
	ctx := context.Background()
	if _, err := a.db.Exec("DELETE FROM " + journalTable); err != nil {
		t.Fatal(err)
	}
	an, err := openAnalytics(ctx, a, dsn)
	if err != nil {
		t.Fatal(err)
	}
	for _, stmt := range []string{"TRUNCATE sessions CASCADE", "TRUNCATE threads CASCADE", "TRUNCATE commits CASCADE", "TRUNCATE prs CASCADE", "TRUNCATE exchanges",
		"TRUNCATE requests", "TRUNCATE state", "TRUNCATE failed"} {
		if _, err = an.db.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}
	a.analytics = an
	t.Cleanup(func() { a.analytics = nil; _ = an.db.Close() })
	return a, an
}

// drain runs the worker until the journal is empty.
func drain(t *testing.T, an *analytics) {
	t.Helper()
	for i := 0; i < 50; i++ {
		more, err := an.step()
		if err != nil {
			t.Fatal(err)
		}
		if !more {
			an.refreshLinks(true) // the worker refreshes request_sessions after draining
			return
		}
	}
	t.Fatal("journal never drained")
}

func count(t *testing.T, an *analytics, q string, args ...any) int {
	t.Helper()
	var n int
	if err := an.db.QueryRow(q, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestAnalyticsImportedLifecycle(t *testing.T) {
	a, an := openTestAnalytics(t)
	ctx := context.Background()
	ts := time.Date(2026, 10, 20, 10, 0, 0, 0, time.UTC)
	png := base64.StdEncoding.EncodeToString(append([]byte("\x89PNG\r\n\x1a\n"), make([]byte, 3000)...))
	usage := `{"provider":"anthropic","model":"claude-opus-5-5","uncached_input":5,"cache_read":900,"cache_write":100,"output":80}`
	msgs := []MessageIngest{
		{ExternalID: "m1", Seq: 1, Role: "user", TS: &ts, Message: json.RawMessage(`{"role":"user","content":[` +
			`{"type":"text","text":"please FULL STOP when done"},` +
			`{"type":"image","source":{"type":"base64","media_type":"image/png","data":"` + png + `"}}]}`)},
		{ExternalID: "m2", Seq: 2, Role: "assistant", TS: &ts, Model: "claude-opus-5-5", Usage: json.RawMessage(usage),
			Message: json.RawMessage(`{"role":"assistant","content":[{"type":"tool_use","id":"tu1","name":"Bash","input":{"command":"rg -n foo"}}]}`)},
		{ExternalID: "m3", Seq: 3, Role: "user", TS: &ts, Message: json.RawMessage(`{"role":"user","content":[` +
			`{"type":"tool_result","tool_use_id":"tu1","is_error":true,"content":"zsh: parse error"}]}`)},
	}
	in := SessionIngest{Harness: "claude-code", ExternalID: "cc-1", Machine: "sonofanton", Title: "Fix",
		StartedAt: &ts, Messages: msgs}
	if _, err := a.IngestSession(ctx, in); err != nil {
		t.Fatal(err)
	}
	drain(t, an)

	if n := count(t, an, `SELECT count(*) FROM messages WHERE session_id = 'claude-code:cc-1'`); n != 3 {
		t.Fatalf("messages = %d", n)
	}
	var msg string
	if err := an.db.QueryRow(`SELECT message::text FROM messages WHERE session_id = 'claude-code:cc-1' AND seq = 0`).Scan(&msg); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(msg, png[:40]) || !strings.Contains(msg, mediaPlaceholderPrefix) {
		t.Fatalf("media not replaced by placeholder: %.200s", msg)
	}
	var name, call string
	var isErr bool
	if err := an.db.QueryRow(`SELECT name, call_id FROM items WHERE kind = 'tool_call'`).Scan(&name, &call); err != nil || name != "Bash" || call != "tu1" {
		t.Fatalf("tool_call = %q %q %v", name, call, err)
	}
	if err := an.db.QueryRow(`SELECT call_id, is_error FROM items WHERE kind = 'tool_result'`).Scan(&call, &isErr); err != nil || call != "tu1" || !isErr {
		t.Fatalf("tool_result = %q %v %v", call, isErr, err)
	}
	if n := count(t, an, `SELECT count(*) FROM requests WHERE starts_with(import_ref, 'claude-code:cc-1#')`); n != 1 {
		t.Fatalf("usage copies = %d", n)
	}

	// Append: earlier rows are kept as they are (digests unchanged).
	var xminBefore, xminAfter string
	_ = an.db.QueryRow(`SELECT xmin::text FROM messages WHERE session_id = 'claude-code:cc-1' AND seq = 0`).Scan(&xminBefore)
	in.Messages = append(in.Messages, MessageIngest{ExternalID: "m4", Seq: 4, Role: "assistant", TS: &ts,
		Message: json.RawMessage(`{"role":"assistant","content":"all good"}`)})
	if _, err := a.IngestSession(ctx, in); err != nil {
		t.Fatal(err)
	}
	drain(t, an)
	_ = an.db.QueryRow(`SELECT xmin::text FROM messages WHERE session_id = 'claude-code:cc-1' AND seq = 0`).Scan(&xminAfter)
	if n := count(t, an, `SELECT messages FROM sessions WHERE id = 'claude-code:cc-1'`); n != 4 || xminBefore != xminAfter {
		t.Fatalf("append: messages=%d, first row rewritten=%v", n, xminBefore != xminAfter)
	}

	// Replace with fewer messages.
	in.Messages, in.Replace = in.Messages[:2], true
	if _, err := a.IngestSession(ctx, in); err != nil {
		t.Fatal(err)
	}
	drain(t, an)
	if n := count(t, an, `SELECT count(*) FROM messages WHERE session_id = 'claude-code:cc-1'`); n != 2 {
		t.Fatalf("after replace messages = %d", n)
	}

	// Search + conversation from analytics.
	hits, err := a.AnalyticsSearch(ctx, SearchQuery{Q: "full stop"})
	if err != nil || len(hits) != 1 || hits[0].SessionID != "claude-code:cc-1" || !strings.Contains(hits[0].Snippet, "FULL STOP") {
		t.Fatalf("search = %+v %v", hits, err)
	}
	conv, err := a.Conversation(ctx, "cc-1", "", "", "")
	if err != nil || conv.Source != "analytics" || conv.Kind != "imported" || len(conv.Items) != 3 {
		t.Fatalf("conversation = %+v %v", conv, err)
	}

	// Delete removes it everywhere.
	if _, err = a.DeleteSession(ctx, "claude-code:cc-1"); err != nil {
		t.Fatal(err)
	}
	drain(t, an)
	if n := count(t, an, `SELECT count(*) FROM sessions`); n != 0 {
		t.Fatalf("sessions after delete = %d", n)
	}
	if n := count(t, an, `SELECT count(*) FROM requests WHERE import_ref IS NOT NULL`); n != 0 {
		t.Fatalf("usage copies after delete = %d", n)
	}
}

func TestAnalyticsProxyShadowAndBackfill(t *testing.T) {
	a, an := openTestAnalytics(t)
	ctx := context.Background()
	start := time.Now().UTC().Add(-time.Minute)
	turn := func(trace, body, resp string, at time.Time) {
		ex := &capture.Exchange{TraceID: trace, SessionID: "canon-1", URL: "/v1/messages", Method: "POST",
			StartedAt: at, FinishedAt: at.Add(time.Second), Status: 200, Streaming: true,
			RequestHeaders: map[string][]string{"X-Claude-Code-Session-Id": {"client-1"}},
			RequestBody:    []byte(body), ResponseBody: []byte(resp)}
		if err := a.storeExchange(ctx, ex); err != nil {
			t.Fatal(err)
		}
	}
	sse := func(text string) string {
		return `data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"` + text + `"}}` + "\n"
	}
	turn("t1", `{"model":"claude-opus-5-5","system":[{"type":"text","text":"You are Claude Code."}],"messages":[{"role":"user","content":"hi there"}]}`, sse("hello"), start)
	drain(t, an)
	if n := count(t, an, `SELECT messages FROM sessions WHERE id = 'proxy:client-1'`); n != 3 { // system, user, response
		t.Fatalf("proxy messages after t1 = %d", n)
	}
	var xminSys1, xminSys2 string
	_ = an.db.QueryRow(`SELECT xmin::text FROM messages WHERE session_id = 'proxy:client-1' AND seq = 0`).Scan(&xminSys1)
	turn("t2", `{"model":"claude-opus-5-5","system":[{"type":"text","text":"You are Claude Code."}],"messages":[{"role":"user","content":"hi there"},{"role":"assistant","content":"hello"},{"role":"user","content":"more"}]}`, sse("sure"), start.Add(5*time.Second))
	drain(t, an)
	_ = an.db.QueryRow(`SELECT xmin::text FROM messages WHERE session_id = 'proxy:client-1' AND seq = 0`).Scan(&xminSys2)
	var title, trace string
	if err := an.db.QueryRow(`SELECT title, trace_id FROM sessions WHERE id = 'proxy:client-1'`).Scan(&title, &trace); err != nil ||
		title != "hi there" || trace != "t2" || xminSys1 != xminSys2 {
		t.Fatalf("proxy session title=%q trace=%q prefix rewritten=%v err=%v", title, trace, xminSys1 != xminSys2, err)
	}
	if n := count(t, an, `SELECT count(*) FROM exchanges`); n != 2 {
		t.Fatalf("exchange copies = %d", n)
	}
	// Found by the canonical id too (Request History links use it).
	conv, err := a.Conversation(ctx, "canon-1", "", "", "proxy")
	if err != nil || conv.Source != "analytics" || len(conv.Exchanges) != 2 || conv.Items[len(conv.Items)-1].Text != "sure" {
		t.Fatalf("proxy conversation = %+v %v", conv, err)
	}

	// Importing the same session shadows the proxied copy.
	if _, err = a.IngestSession(ctx, SessionIngest{Harness: "claude-code", ExternalID: "client-1",
		Messages: []MessageIngest{{ExternalID: "m1", Seq: 1, Role: "user", Message: json.RawMessage(`{"role":"user","content":"hi there"}`)}}}); err != nil {
		t.Fatal(err)
	}
	drain(t, an)
	var shadow string
	if err = an.db.QueryRow(`SELECT coalesce(shadowed_by, '') FROM sessions WHERE id = 'proxy:client-1'`).Scan(&shadow); err != nil || shadow != "claude-code:client-1" {
		t.Fatalf("shadowed_by = %q %v", shadow, err)
	}
	if hits, _ := a.AnalyticsSearch(ctx, SearchQuery{Q: "hi there"}); len(hits) != 1 || hits[0].SessionKind != "imported" {
		t.Fatalf("search should skip shadowed proxy copy: %+v", hits)
	}

	// Backfill rebuilds everything from scratch.
	for _, stmt := range []string{"TRUNCATE sessions CASCADE", "TRUNCATE exchanges", "TRUNCATE requests"} {
		if _, err = an.db.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}
	res, err := a.AnalyticsBackfill(ctx)
	if err != nil || res.Sessions != 1 || res.Proxy != 1 || res.Exchanges != 2 {
		t.Fatalf("backfill = %+v %v", res, err)
	}
	drain(t, an)
	an.buildDeferredIndexes()
	if n := count(t, an, `SELECT count(*) FROM pg_indexes WHERE indexname IN ('items_text_trgm_idx', 'items_prose_trgm_idx')`); n != 2 {
		t.Fatal("trigram index not rebuilt after backfill")
	}
	if n := count(t, an, `SELECT count(*) FROM sessions`); n != 2 {
		t.Fatalf("sessions after backfill = %d", n)
	}
	if n := count(t, an, `SELECT count(*) FROM items WHERE session_id = 'proxy:client-1'`); n != 5 {
		t.Fatalf("proxy items after backfill = %d", n)
	}
	st, err := a.AnalyticsStatus(ctx)
	if err != nil || st["journal_backlog"].(int64) != 0 {
		t.Fatalf("status = %+v %v", st, err)
	}
}

// A journal entry whose transaction commits after a later entry's must still be processed.
func TestAnalyticsJournalOutOfOrderCommit(t *testing.T) {
	a, an := openTestAnalytics(t)
	ctx := context.Background()
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = a.journal(ctx, tx, "session", "late:1"); err != nil { // lower id, commits last
		t.Fatal(err)
	}
	if err = a.journal(ctx, a.db, "session", "early:2"); err != nil {
		t.Fatal(err)
	}
	drain(t, an)
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var left int
	if err = a.db.QueryRow(`SELECT count(*) FROM ` + journalTable).Scan(&left); err != nil || left != 1 {
		t.Fatalf("journal after first drain = %d %v", left, err)
	}
	drain(t, an)
	if err = a.db.QueryRow(`SELECT count(*) FROM ` + journalTable).Scan(&left); err != nil || left != 0 {
		t.Fatalf("late entry not processed: %d left %v", left, err)
	}
}

func TestSanitizeJSONEscapes(t *testing.T) {
	for in, want := range map[string]string{
		`{"a":"x\u0000y"}`:           `{"a":"x�y"}`,
		`{"a":"lit \\u0000 text"}`:   `{"a":"lit \\u0000 text"}`, // escaped backslash: literal text
		`{"a":"😀 ok"}`:               `{"a":"😀 ok"}`,             // valid pair kept
		`{"a":"lone \ud83d end"}`:    `{"a":"lone � end"}`,
		`{"a":"low \ude00"}`:         `{"a":"low �"}`,
		`{"n":1.000000000000000001}`: `{"n":1.000000000000000001}`, // numbers untouched
	} {
		if got := string(cleanJSON([]byte(in))); got != want {
			t.Errorf("cleanJSON(%s) = %s, want %s", in, got, want)
		}
	}
}

func TestAnalyticsStoresHostileJSON(t *testing.T) {
	a, an := openTestAnalytics(t)
	ctx := context.Background()
	if _, err := a.IngestSession(ctx, SessionIngest{Harness: "opencode", ExternalID: "nul-1", Messages: []MessageIngest{
		{ExternalID: "m1", Seq: 1, Role: "user", Message: json.RawMessage(`{"role":"user","content":"bad \u0000 and \ud83d here, literal \\u0000"}`)},
	}}); err != nil {
		t.Fatal(err)
	}
	drain(t, an)
	var text string
	if err := an.db.QueryRow(`SELECT text FROM items WHERE session_id = 'opencode:nul-1'`).Scan(&text); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(text, `literal \u0000`) || strings.ContainsRune(text, 0) {
		t.Fatalf("text = %q", text)
	}
	if n := count(t, an, `SELECT count(*) FROM messages WHERE session_id = 'opencode:nul-1' AND jsonb_typeof(message) = 'object'`); n != 1 {
		t.Fatal("message not stored as a JSON object")
	}
}

// tmx.requests changes reach the copy through the trigger journal, whatever wrote them.
func TestAnalyticsRequestRowsFollowSource(t *testing.T) {
	a, an := openTestAnalytics(t)
	rl := an.reqMirror
	if rl == nil {
		t.Fatal("requests mirror not installed")
	}
	ctx := context.Background()
	tx, err := rl.src.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	var lateID int64
	if err = tx.QueryRow(`INSERT INTO ` + rl.srcTable + ` (ts, outcome, model) VALUES (now(), 'success', 'late') RETURNING id`).Scan(&lateID); err != nil {
		t.Fatal(err)
	}
	var earlyID int64
	if err = rl.src.QueryRow(`INSERT INTO ` + rl.srcTable + ` (ts, outcome, model) VALUES (now(), 'success', 'early') RETURNING id`).Scan(&earlyID); err != nil {
		t.Fatal(err)
	}
	drain(t, an)
	if err = tx.Commit(); err != nil { // commits after a higher id was already copied
		t.Fatal(err)
	}
	drain(t, an)
	if n := count(t, an, `SELECT count(*) FROM requests WHERE model IN ('late', 'early')`); n != 2 {
		t.Fatalf("copies = %d", n)
	}
	if _, err = rl.src.Exec(`UPDATE `+rl.srcTable+` SET model = 'late2' WHERE id = $1`, lateID); err != nil {
		t.Fatal(err)
	}
	if _, err = rl.src.Exec(`DELETE FROM `+rl.srcTable+` WHERE id = $1`, earlyID); err != nil {
		t.Fatal(err)
	}
	drain(t, an)
	if n := count(t, an, `SELECT count(*) FROM requests WHERE model = 'late2'`); n != 1 {
		t.Fatal("update not copied")
	}
	if n := count(t, an, `SELECT count(*) FROM requests WHERE id = $1`, earlyID); n != 0 {
		t.Fatal("delete not copied")
	}
	// TRUNCATE fires no row triggers; its statement trigger queues a full reconcile.
	if _, err = rl.src.Exec(`TRUNCATE ` + rl.srcTable); err != nil {
		t.Fatal(err)
	}
	drain(t, an)
	if n := count(t, an, `SELECT count(*) FROM requests`); n != 0 {
		t.Fatalf("copies after truncate = %d", n)
	}
	_ = a
}
