package archive

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/capture"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/requestlog"
)

// request_sessions follows changes that arrive in any order, through the incremental update
// alone: a usage row before its exchange, a transcript import that shadows the proxied session,
// and that transcript's deletion. drain checks it against a full recompute every time.
func TestRequestLinksIncremental(t *testing.T) {
	a, an := openTestAnalytics(t)
	ctx := context.Background()
	rl := requestlog.Default()
	if rl == nil || an.reqMirror == nil {
		t.Skip("request log not mirrored")
	}
	drain(t, an) // the first refresh is the full one
	at := time.Now().UTC().Add(-time.Minute)
	usage := func(trace, session string) {
		rl.Enqueue(requestlog.Row{TS: at, TraceID: trace, SessionID: session, Source: requestlog.SourceProxy,
			Provider: "claude", Model: "claude-opus-5-5", Outcome: requestlog.OutcomeSuccess, TotalTokens: 10})
		if err := rl.FlushContext(ctx); err != nil {
			t.Fatal(err)
		}
	}
	link := func(trace string) (string, string) {
		t.Helper()
		var sess, canon string
		if err := an.db.QueryRow(`SELECT COALESCE(rs.session_id, ''), COALESCE(rs.canonical_session_id, '')
			FROM request_sessions rs JOIN requests r ON r.id = rs.request_id WHERE r.trace_id = $1`, trace).Scan(&sess, &canon); err != nil {
			t.Fatalf("link of %s: %v", trace, err)
		}
		return sess, canon
	}
	// Usage first: nothing to link to yet. lt2 carries no session id, so only its trace links it.
	usage("lt1", "client-1")
	usage("lt2", "")
	drain(t, an)
	if s, _ := link("lt1"); s != "" {
		t.Fatalf("lt1 linked before its session exists: %q", s)
	}
	for _, tr := range []string{"lt1", "lt2"} {
		if err := a.storeExchange(ctx, &capture.Exchange{TraceID: tr, SessionID: "canon-1", URL: "/v1/messages", Method: "POST",
			StartedAt: at, FinishedAt: at.Add(time.Second), Status: 200,
			RequestHeaders: map[string][]string{"X-Claude-Code-Session-Id": {"client-1"}},
			RequestBody:    []byte(`{"model":"claude-opus-5-5","messages":[{"role":"user","content":"hi"}]}`)}); err != nil {
			t.Fatal(err)
		}
	}
	drain(t, an)
	if s, c := link("lt2"); s != "proxy:client-1" || c != "proxy:client-1" {
		t.Fatalf("lt2 after its exchange = %q %q", s, c)
	}
	// The transcript arrives: both rows now count for it.
	if _, err := a.IngestSession(ctx, SessionIngest{Harness: "claude-code", ExternalID: "client-1",
		Messages: []MessageIngest{{ExternalID: "m1", Seq: 1, Role: "user", Message: json.RawMessage(`{"role":"user","content":"hi"}`)}}}); err != nil {
		t.Fatal(err)
	}
	drain(t, an)
	for _, tr := range []string{"lt1", "lt2"} {
		if _, c := link(tr); c != "claude-code:client-1" {
			t.Fatalf("%s canonical after import = %q", tr, c)
		}
	}
	// And back when it is deleted.
	if _, err := a.DeleteSession(ctx, "claude-code:client-1"); err != nil {
		t.Fatal(err)
	}
	drain(t, an)
	if _, c := link("lt2"); c != "proxy:client-1" {
		t.Fatalf("lt2 canonical after delete = %q", c)
	}
}

// Search widens its window until a page is full, keeps newest-first order across windows, and
// matches the query literally and case-insensitively.
func TestAnalyticsSearchWindows(t *testing.T) {
	a, an := openTestAnalytics(t)
	ctx := context.Background()
	now := time.Now().UTC()
	var msgs []MessageIngest
	for i, age := range []time.Duration{time.Hour, 20 * 24 * time.Hour, 200 * 24 * time.Hour, 3 * 365 * 24 * time.Hour} {
		ts := now.Add(-age)
		msgs = append(msgs, MessageIngest{ExternalID: "m" + string(rune('a'+i)), Seq: int64(i + 1), Role: "user", TS: &ts,
			Message: json.RawMessage(`{"role":"user","content":"Needle a.b(c number ` + string(rune('0'+i)) + `"}`)})
	}
	decoy := now.Add(-time.Minute)
	msgs = append(msgs, MessageIngest{ExternalID: "decoy", Seq: 9, Role: "user", TS: &decoy,
		Message: json.RawMessage(`{"role":"user","content":"needle axb(c"}`)})
	if _, err := a.IngestSession(ctx, SessionIngest{Harness: "opencode", ExternalID: "hay", Messages: msgs}); err != nil {
		t.Fatal(err)
	}
	drain(t, an)
	texts := func(q SearchQuery) []string {
		t.Helper()
		hits, err := a.AnalyticsSearch(ctx, q)
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, h := range hits {
			out = append(out, h.Snippet)
		}
		return out
	}
	// "a.b(c" is literal: the decoy "axb(c" must not match; case is ignored.
	got := texts(SearchQuery{Q: "NEEDLE A.B(C", Limit: 10})
	if len(got) != 4 || got[0] != "Needle a.b(c number 0" || got[3] != "Needle a.b(c number 3" {
		t.Fatalf("all windows = %q", got)
	}
	// A page filled by the first window stops there; one that isn't, widens.
	if got = texts(SearchQuery{Q: "needle a.b", Limit: 1}); len(got) != 1 || got[0] != "Needle a.b(c number 0" {
		t.Fatalf("limit 1 = %q", got)
	}
	if got = texts(SearchQuery{Q: "needle a.b", Limit: 3}); len(got) != 3 || got[2] != "Needle a.b(c number 2" {
		t.Fatalf("limit 3 = %q", got)
	}
	// from bounds the search; to anchors the windows.
	if got = texts(SearchQuery{Q: "needle a.b", From: now.Add(-30 * 24 * time.Hour), Limit: 10}); len(got) != 2 {
		t.Fatalf("from = %q", got)
	}
	if got = texts(SearchQuery{Q: "needle a.b", To: now.Add(-24 * time.Hour), Limit: 10}); len(got) != 3 || got[0] != "Needle a.b(c number 1" {
		t.Fatalf("to = %q", got)
	}
	// No text: filters alone, newest first.
	if got = texts(SearchQuery{SessionID: "opencode:hay", Limit: 2}); len(got) != 2 || got[0] != "needle axb(c" {
		t.Fatalf("no query = %q", got)
	}
}
