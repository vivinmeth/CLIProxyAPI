package archive

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/capture"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/pricing"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/requestlog"
)

// Runs against a real Postgres when TMX_REQUESTLOG_TEST_DSN is set (throwaway container). No R2:
// blobs stay in a temp spool, which exercises the same read path.
func openTestArchive(t *testing.T) (*Archive, *requestlog.Store) {
	t.Helper()
	dsn := os.Getenv("TMX_REQUESTLOG_TEST_DSN")
	if dsn == "" {
		t.Skip("TMX_REQUESTLOG_TEST_DSN not set")
	}
	ctx := context.Background()
	a, err := Open(ctx, dsn, nil, "unused", "dev", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, stmt := range []string{
		"DELETE FROM " + schema + ".session_sources", "DELETE FROM " + schema + ".session_messages",
		"DELETE FROM " + schema + ".sessions", "DELETE FROM " + schema + ".exchanges",
		"DELETE FROM " + schema + ".blobs", "DELETE FROM " + schema + ".threads", "DELETE FROM " + schema + ".commits",
		"DELETE FROM " + schema + ".pull_requests", "DELETE FROM " + schema + ".pull_request_versions",
	} {
		if _, err = a.db.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("TMX_PRICES", "off")
	t.Setenv("WRITABLE_PATH", t.TempDir())
	if _, err = a.db.Exec("DROP TABLE IF EXISTS tmx.price_catalogs"); err != nil {
		t.Fatal(err)
	}
	rl, err := requestlog.Open(ctx, dsn, "tmx", 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = rl.DB().Exec("DELETE FROM tmx.requests"); err != nil {
		t.Fatal(err)
	}
	cat, err := pricing.ParseModelsDev([]byte(`{"anthropic":{"models":{"claude-opus-5-5":{"cost":{"input":4,"output":20,"cache_read":0.2,"cache_write":5}}}}}`))
	if err != nil {
		t.Fatal(err)
	}
	rl.SetPriceCatalogForTest(cat, time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC))
	requestlog.SetDefaultForTest(rl)
	t.Cleanup(func() {
		requestlog.SetDefaultForTest(nil)
		a.Close()
		rl.Close()
	})
	return a, rl
}

func countBlobs(t *testing.T, a *Archive) int {
	var n int
	if err := a.db.QueryRow("SELECT count(*) FROM " + schema + ".blobs").Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestSplitRebuildDedupAndMedia(t *testing.T) {
	a, _ := openTestArchive(t)
	ctx := context.Background()
	png := append([]byte("\x89PNG\r\n\x1a\n"), bytes.Repeat([]byte{7}, 4000)...)
	img := base64.StdEncoding.EncodeToString(png)
	turn1 := `{"model":"claude-opus-5-5","max_tokens":32000,"system":[{"type":"text","text":"` +
		strings.Repeat("You are Claude Code. ", 60) + `","cache_control":{"type":"ephemeral"}}],` +
		`"messages":[{"role":"user","content":[{"type":"image","source":{"type":"base64","media_type":"image/png","data":"` + img + `"}},` +
		`{"type":"text","text":"what is this?","cache_control":{"type":"ephemeral"}}]}],"stream":true}`
	// Turn 2 resends turn 1 with the cache marker moved to the newest message.
	turn2 := `{"model":"claude-opus-5-5","max_tokens":32000,"system":[{"type":"text","text":"` +
		strings.Repeat("You are Claude Code. ", 60) + `","cache_control":{"type":"ephemeral"}}],` +
		`"messages":[{"role":"user","content":[{"type":"image","source":{"type":"base64","media_type":"image/png","data":"` + img + `"}},` +
		`{"type":"text","text":"what is this?"}]},{"role":"assistant","content":[{"type":"text","text":"A test image."}]},` +
		`{"role":"user","content":[{"type":"text","text":"thanks","cache_control":{"type":"ephemeral"}}]}],"stream":true}`

	h1, err := a.splitBody(ctx, []byte(turn1), "request")
	if err != nil {
		t.Fatal(err)
	}
	afterTurn1 := countBlobs(t, a)
	h2, err := a.splitBody(ctx, []byte(turn2), "request")
	if err != nil {
		t.Fatal(err)
	}
	// Turn 2 adds: 2 new messages + its manifest. The system block, the first user message
	// (cache marker stripped) and the image are reused.
	if added := countBlobs(t, a) - afterTurn1; added != 3 {
		t.Fatalf("turn 2 added %d blobs, want 3", added)
	}
	var mediaType string
	if err = a.db.QueryRow("SELECT media_type FROM "+schema+".blobs WHERE hash = $1", HashOf(png)).Scan(&mediaType); err != nil || mediaType != "image/png" {
		t.Fatalf("image blob: %q %v", mediaType, err)
	}
	for _, c := range []struct{ hash, want string }{{h1, turn1}, {h2, turn2}} {
		got, errRebuild := a.rebuildBody(ctx, c.hash)
		if errRebuild != nil {
			t.Fatal(errRebuild)
		}
		var wantV, gotV any
		_ = json.Unmarshal([]byte(c.want), &wantV)
		if errJSON := json.Unmarshal(got, &gotV); errJSON != nil {
			t.Fatalf("rebuilt body not JSON: %v", errJSON)
		}
		w, _ := json.Marshal(wantV)
		g, _ := json.Marshal(gotV)
		if !bytes.Equal(w, g) {
			t.Fatalf("rebuild mismatch\nwant %s\ngot  %s", w[:200], g[:200])
		}
	}
	// Non-JSON bodies are stored raw.
	hr, err := a.splitBody(ctx, []byte("not json at all"), "request")
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := a.rebuildBody(ctx, hr); string(got) != "not json at all" {
		t.Fatalf("raw rebuild = %q", got)
	}
}

func TestExchangeRoundTrip(t *testing.T) {
	a, _ := openTestArchive(t)
	ctx := context.Background()
	start := time.Now().Add(-2 * time.Second)
	ex := &capture.Exchange{
		TraceID: "trace-1", SessionID: "sess-1", URL: "/v1/messages", Method: "POST",
		StartedAt: start, FinishedAt: start.Add(1500 * time.Millisecond), FirstChunkAt: start.Add(300 * time.Millisecond),
		Status: 200, Streaming: true,
		RequestHeaders:  map[string][]string{"Authorization": {"Bearer sk-secret"}, "Anthropic-Version": {"2023-06-01"}},
		RequestBody:     []byte(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`),
		ResponseHeaders: map[string][]string{"Content-Type": {"text/event-stream"}},
		ResponseBody:    []byte("event: message_start\ndata: {}\n\n"),
		Chunks:          []capture.ChunkMark{{OffsetMs: 300, Size: 10}, {OffsetMs: 900, Size: 20}},
		APIRequest:      []byte(`{"model":"m","messages":[{"role":"user","content":"hi"}],"system":"injected"}`),
		APIResponse:     []byte("upstream sse"),
		Errors:          []string{},
	}
	if err := a.storeExchange(ctx, ex); err != nil {
		t.Fatal(err)
	}
	out, err := a.GetExchange(ctx, "trace-1")
	if err != nil {
		t.Fatal(err)
	}
	meta := out["meta"].(*ExchangeMeta)
	if meta.FirstChunkMs != 300 || meta.DurationMs != 1500 || meta.SessionID != "sess-1" {
		t.Fatalf("meta = %+v", meta)
	}
	if !strings.Contains(string(out["request_headers"].(json.RawMessage)), "[redacted]") ||
		strings.Contains(string(out["request_headers"].(json.RawMessage)), "sk-secret") {
		t.Fatal("credential header not redacted")
	}
	if out["response_body"] != string(ex.ResponseBody) || out["upstream_response"] != "upstream sse" {
		t.Fatalf("bodies = %v / %v", out["response_body"], out["upstream_response"])
	}
	if !strings.Contains(string(out["upstream_request"].(json.RawMessage)), "injected") {
		t.Fatal("upstream request not rebuilt")
	}
}

func TestIngestSessionIdempotentWithUsage(t *testing.T) {
	a, rl := openTestArchive(t)
	ctx := context.Background()
	source := []byte(`{"type":"user","uuid":"m1"}` + "\n" + `{"type":"assistant","uuid":"m2"}` + "\n")
	ts := time.Date(2026, 10, 20, 20, 45, 0, 0, time.UTC) // 02:15 IST on 10/21
	usage := `{"provider":"anthropic","model":"claude-opus-5-5","account":"me@example.com","uncached_input":5,"cache_read":900,"cache_write":100,"output":80,"reasoning":30}`
	in := SessionIngest{
		Harness: "claude-code", ExternalID: "abc-123", Machine: "laptop", HarnessVersion: "2.1",
		StartedAt: &ts,
		Messages: []MessageIngest{
			{ExternalID: "m1", Seq: 1, Role: "user", TS: &ts, Message: json.RawMessage(`{"role":"user","content":"hello"}`)},
			{ExternalID: "m2", Seq: 2, Role: "assistant", TS: &ts, Model: "claude-opus-5-5",
				Message: json.RawMessage(`{"role":"assistant","content":[{"type":"text","text":"hi"}]}`),
				Usage:   json.RawMessage(usage)},
		},
		Sources: []SourceIngest{{Name: "abc-123.jsonl", Blob: HashOf(source)}},
	}
	if _, err := a.IngestSession(ctx, in); err == nil {
		t.Fatal("expected missing source blob error")
	}
	if _, err := a.blobs.Put(ctx, source, "application/jsonl", "import.source"); err != nil {
		t.Fatal(err)
	}
	res, err := a.IngestSession(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Created || res.Inserted != 2 || res.UsageRows != 1 || res.Sources != 1 {
		t.Fatalf("first ingest = %+v", res)
	}
	res, err = a.IngestSession(ctx, in)
	if err != nil || res.Created || res.Unchanged != 2 || res.Inserted != 0 || res.Sources != 0 {
		t.Fatalf("re-ingest = %+v %v", res, err)
	}
	in.Messages = append(in.Messages, MessageIngest{ExternalID: "m3", Seq: 3, Role: "user", TS: &ts,
		Message: json.RawMessage(`{"role":"user","content":"more"}`)})
	if res, err = a.IngestSession(ctx, in); err != nil || res.Inserted != 1 || res.Unchanged != 2 {
		t.Fatalf("incremental ingest = %+v %v", res, err)
	}
	got, err := a.GetSession(ctx, SessionKey("claude-code", "abc-123"))
	if err != nil {
		t.Fatal(err)
	}
	if msgs := got["messages"].([]StoredMessage); len(msgs) != 3 || !strings.Contains(string(msgs[1].Message), `"hi"`) {
		t.Fatalf("messages = %+v", msgs)
	}

	// Usage row: once, under extern:claude-code, normalised.
	f := requestlog.Filter{From: ts.Add(-time.Hour), To: ts.Add(time.Hour)}
	items, err := rl.List(ctx, f, 0, 10)
	if err != nil || len(items) != 1 {
		t.Fatalf("usage rows = %d %v", len(items), err)
	}
	r := items[0]
	if r.Source != "extern:claude-code" || r.CacheRead != 900 || r.OutputNonReasoning != 50 || r.ReasoningTokens != 30 ||
		r.Machine != "laptop" || r.Shadowed || r.ImportRef != "claude-code:abc-123#m2" {
		t.Fatalf("usage row = %+v", r.Row)
	}

	// Same session also captured by the proxy -> imported usage becomes shadowed.
	if err = a.storeExchange(ctx, &capture.Exchange{TraceID: "t-proxy", SessionID: "canonical-other-id", StartedAt: ts,
		RequestHeaders: map[string][]string{"X-Claude-Code-Session-Id": {"abc-123"}}}); err != nil {
		t.Fatal(err)
	}
	in2 := in
	in2.Messages = in.Messages[:2]
	if res, err = a.IngestSession(ctx, in2); err != nil || res.ViaProxy == nil || !*res.ViaProxy {
		t.Fatalf("linked ingest = %+v %v", res, err)
	}
	if items, _ = rl.List(ctx, f, 0, 10); len(items) != 0 {
		t.Fatalf("shadowed rows must be hidden by default, got %d", len(items))
	}
	f.IncludeShadowed = true
	if items, _ = rl.List(ctx, f, 0, 10); len(items) != 1 || !items[0].Shadowed {
		t.Fatalf("include_shadowed = %+v", items)
	}

	// Rollup in IST puts 20:45 UTC on 10/21.
	cells, _, err := rl.Rollup(ctx, f, "day", "Asia/Kolkata", []string{"source", "model"}, 100)
	if err != nil || len(cells) != 1 {
		t.Fatalf("rollup = %+v %v", cells, err)
	}
	if day := cells[0].Start.In(time.FixedZone("IST", 19800)).Format("2006-01-02 15:04"); day != "2026-10-21 00:00" {
		t.Fatalf("IST day bucket = %s", day)
	}
	if cells[0].Dims["source"] != "extern:claude-code" || cells[0].CacheRead != 900 {
		t.Fatalf("cell = %+v", cells[0])
	}
	f.Sources = []string{"tmx-proxy"}
	if cells, _, _ = rl.Rollup(ctx, f, "day", "UTC", nil, 100); len(cells) != 0 {
		t.Fatalf("source filter leaked: %+v", cells)
	}
	f.Sources = []string{"extern:*"}
	if cells, _, _ = rl.Rollup(ctx, f, "day", "UTC", nil, 100); len(cells) != 1 {
		t.Fatalf("extern:* filter = %+v", cells)
	}
	_ = fmt.Sprintf
}
