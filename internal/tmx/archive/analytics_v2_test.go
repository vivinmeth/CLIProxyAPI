package archive

import (
	"context"
	"encoding/json"
	"math"
	"os"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/requestlog"
	"github.com/tidwall/gjson"
)

func TestNormalizeOpenCodeParts(t *testing.T) {
	msg := `{"role":"assistant","content":[
	{"type":"step-start","snapshot":"abc"},
	{"type":"reasoning","text":"look first"},
	{"type":"tool","tool":"bash","callID":"c1","state":{"status":"completed","input":{"command":"ls -la"},"output":"a.go"}},
	{"type":"tool","tool":"edit","callID":"c2","state":{"status":"error","input":{"filePath":"x"},"error":"no such file"}},
	{"type":"tool","tool":"read","callID":"c3","state":{"status":"running","input":{"filePath":"y"}}},
	{"type":"text","text":"done"},
	{"type":"step-finish","reason":"stop","cost":0.01,"tokens":{"input":5}}]}`
	items := normalizeMessage(gjson.Parse(msg), "assistant")
	want := "[assistant:thinking assistant:tool_call tool:tool_result assistant:tool_call tool:tool_result assistant:tool_call assistant:text assistant:other]"
	if got := fmtSlice(kinds(items)); got != want {
		t.Fatalf("items = %v, want %v", got, want)
	}
	if items[1].Name != "bash" || items[1].CallID != "c1" || gjson.Get(items[1].Data, "command").String() != "ls -la" {
		t.Fatalf("bash call = %+v", items[1])
	}
	if items[2].CallID != "c1" || items[2].Text != "a.go" || items[2].IsError {
		t.Fatalf("bash result = %+v", items[2])
	}
	if !items[4].IsError || items[4].Text != "no such file" || items[4].CallID != "c2" {
		t.Fatalf("edit result = %+v", items[4])
	}
	if gjson.Get(items[7].Data, "cost").Float() != 0.01 {
		t.Fatalf("step-finish data = %q", items[7].Data)
	}
}

func TestNormalizeCursorBubble(t *testing.T) {
	bubble := `{"_v":1,"type":2,"bubbleId":"b1","text":"","thinking":{"text":"plan it"},
	"toolFormerData":{"name":"run_terminal_cmd","rawArgs":"{\"command\": \"go test ./...\"}","params":"{}",
	"result":"{\"output\":\"FAIL\"}","status":"error","toolCallId":"t1"},"codeBlocks":[],"relevantFiles":["a"]}`
	items := normalizeMessage(gjson.Parse(bubble), "assistant")
	if got := fmtSlice(kinds(items)); got != "[assistant:thinking assistant:tool_call tool:tool_result]" {
		t.Fatalf("items = %v", got)
	}
	if items[1].Name != "run_terminal_cmd" || gjson.Get(items[1].Data, "command").String() != "go test ./..." || items[1].CallID != "t1" {
		t.Fatalf("call = %+v", items[1])
	}
	if !items[2].IsError || items[2].CallID != "t1" {
		t.Fatalf("result = %+v", items[2])
	}
	user := normalizeMessage(gjson.Parse(`{"type":1,"bubbleId":"b0","text":"fix the tests"}`), "")
	if len(user) != 1 || user[0].Role != "user" || user[0].Text != "fix the tests" {
		t.Fatalf("user bubble = %+v", user)
	}
	if empty := normalizeMessage(gjson.Parse(`{"type":2,"bubbleId":"b2","text":""}`), "assistant"); len(empty) != 0 {
		t.Fatalf("empty bubble = %+v", empty)
	}
}

func TestToolCallDataKeepsWholeArguments(t *testing.T) {
	big := make([]byte, convMaxItemText+100)
	for i := range big {
		big[i] = 'x'
	}
	args, _ := json.Marshal(map[string]string{"content": string(big), "file_path": "/a"})
	items := normalizeMessage(gjson.Parse(`{"role":"assistant","content":[{"type":"tool_use","id":"t","name":"Write","input":`+string(args)+`}]}`), "")
	if len(items[0].Text) > convMaxItemText+20 || gjson.Get(items[0].Data, "file_path").String() != "/a" {
		t.Fatalf("text %d bytes, data path %q", len(items[0].Text), gjson.Get(items[0].Data, "file_path").String())
	}
}

func TestFreeFormToolInputKeptAsString(t *testing.T) {
	items := normalizeMessage(gjson.Parse(`{"type":"custom_tool_call","name":"apply_patch","call_id":"c","input":"*** Begin Patch\n*** End Patch"}`), "")
	if gjson.Parse(items[0].Data).String() != "*** Begin Patch\n*** End Patch" {
		t.Fatalf("data = %q", items[0].Data)
	}
}

// Usage rows link to their session and message, get a list price, and roll up per session;
// tool calls keep their arguments as JSON; an extractor version bump rewrites the session.
func TestAnalyticsCostsLinksAndVersion(t *testing.T) {
	a, an := openTestAnalytics(t)
	ctx := context.Background()
	ts := time.Date(2026, 10, 20, 10, 0, 0, 0, time.UTC)
	usage := `{"provider":"anthropic","model":"claude-opus-5-5","uncached_input":5,"cache_read":900,"cache_write":100,"cache_write_1h":100,"output":80}`
	in := SessionIngest{Harness: "claude-code", ExternalID: "cc-cost", Machine: "alien", StartedAt: &ts,
		Messages: []MessageIngest{
			{ExternalID: "u1", Seq: 1, Role: "user", TS: &ts, Message: json.RawMessage(`{"role":"user","content":"go"}`)},
			{ExternalID: "a1", Seq: 2, Role: "assistant", TS: &ts, Model: "claude-opus-5-5", Usage: json.RawMessage(usage),
				Message: json.RawMessage(`{"role":"assistant","content":[{"type":"tool_use","id":"tu","name":"Bash","input":{"command":"make test"}}]}`)},
		}}
	if _, err := a.IngestSession(ctx, in); err != nil {
		t.Fatal(err)
	}
	drain(t, an)

	var cmd string
	if err := an.db.QueryRow(`SELECT data->>'command' FROM items WHERE kind = 'tool_call'`).Scan(&cmd); err != nil || cmd != "make test" {
		t.Fatalf("tool_call data = %q %v", cmd, err)
	}
	var sid, canon, via string
	var seq int
	if err := an.db.QueryRow(`SELECT session_id, canonical_session_id, message_seq, via FROM request_sessions`).Scan(&sid, &canon, &seq, &via); err != nil ||
		sid != "claude-code:cc-cost" || canon != sid || seq != 1 || via != "import_ref" {
		t.Fatalf("link = %q %q %d %q %v", sid, canon, seq, via, err)
	}
	// Priced by the request log when written (catalog installed by openTestArchive: Opus 5.5 at
	// 4 / 20, read 0.2, write 5; the 100 written tokens are 1-hour: 2x input).
	want := (5*4 + 900*0.2 + 100*8 + 80*20) / 1e6
	var cost float64
	if err := an.db.QueryRow(`SELECT cost_usd FROM request_costs`).Scan(&cost); err != nil || math.Abs(cost-want) > 1e-12 {
		t.Fatalf("cost = %v, want %v (%v)", cost, want, err)
	}
	if err := an.db.QueryRow(`SELECT cost_usd FROM session_usage WHERE session_id = 'claude-code:cc-cost'`).Scan(&cost); err != nil || math.Abs(cost-want) > 1e-12 {
		t.Fatalf("session cost = %v (%v)", cost, err)
	}

	// Extracted by an older version: the next pass rewrites every message.
	var before, after string
	_ = an.db.QueryRow(`SELECT xmin::text FROM messages WHERE session_id = 'claude-code:cc-cost' AND seq = 0`).Scan(&before)
	if _, err := an.db.Exec(`UPDATE sessions SET version = 1`); err != nil {
		t.Fatal(err)
	}
	if err := a.journal(ctx, a.db, "session", "claude-code:cc-cost"); err != nil {
		t.Fatal(err)
	}
	drain(t, an)
	_ = an.db.QueryRow(`SELECT xmin::text FROM messages WHERE session_id = 'claude-code:cc-cost' AND seq = 0`).Scan(&after)
	if before == after || count(t, an, `SELECT version FROM sessions WHERE id = 'claude-code:cc-cost'`) != analyticsVersion {
		t.Fatalf("old-version session not rewritten (xmin %s -> %s)", before, after)
	}
	// An old-version session whose transcript is now empty loses its old rows too.
	if _, err := an.db.Exec(`UPDATE sessions SET version = 1`); err != nil {
		t.Fatal(err)
	}
	if err := an.writeSession(ctx, sessionRow{id: "claude-code:cc-cost", kind: "imported"}, nil); err != nil {
		t.Fatal(err)
	}
	if n := count(t, an, `SELECT count(*) FROM messages WHERE session_id = 'claude-code:cc-cost'`); n != 0 {
		t.Fatalf("emptied old-version session kept %d messages", n)
	}
}

// A blob indexed after the upload pass read the spool must stay spooled (and go up next pass),
// and an index row whose blob is gone is swept so a re-import can store it again.
func TestPackUploadKeepsUnindexedSpoolAndSweeps(t *testing.T) {
	endpoint := os.Getenv("TMX_ARCHIVE_TEST_S3")
	dsn := os.Getenv("TMX_REQUESTLOG_TEST_DSN")
	if endpoint == "" || dsn == "" {
		t.Skip("TMX_ARCHIVE_TEST_S3 / TMX_REQUESTLOG_TEST_DSN not set")
	}
	ctx := context.Background()
	s3, err := minio.New(endpoint, &minio.Options{Creds: credentials.NewStaticV4("minioadmin", "minioadmin", "")})
	if err != nil {
		t.Fatal(err)
	}
	bucket := "tmx-race-test"
	if exists, _ := s3.BucketExists(ctx, bucket); !exists {
		if err = s3.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	a, err := Open(ctx, dsn, s3, bucket, "test", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b := a.blobs
	if _, err = a.db.Exec("DELETE FROM " + schema + ".blobs"); err != nil {
		t.Fatal(err)
	}
	// Put's first half: spooled, row not committed yet.
	data := []byte("in-flight blob")
	hash := HashOf(data)
	compressed := b.enc.EncodeAll(data, nil)
	if err = os.WriteFile(b.spoolPath(hash), compressed, 0o640); err != nil {
		t.Fatal(err)
	}
	b.uploadPending()
	if _, errStat := os.Stat(b.spoolPath(hash)); errStat != nil {
		t.Fatal("unindexed blob was removed from the spool")
	}
	// Put's second half lands; the next pass uploads it.
	if _, err = a.db.Exec(`INSERT INTO `+schema+`.blobs (hash, size, stored_size, media_type, kind) VALUES ($1, $2, $3, '', 'test')`, hash, len(data), len(compressed)); err != nil {
		t.Fatal(err)
	}
	b.uploadPending()
	var uploaded bool
	if err = a.db.QueryRow(`SELECT uploaded_at IS NOT NULL FROM `+schema+`.blobs WHERE hash = $1`, hash).Scan(&uploaded); err != nil || !uploaded {
		t.Fatalf("blob not uploaded on the next pass (%v)", err)
	}
	got, err := b.Get(ctx, hash)
	if err != nil || string(got) != string(data) {
		t.Fatalf("read back = %q %v", got, err)
	}

	// An old row that was never uploaded and has no spool file is swept; Put stores it again.
	lost := []byte("lost blob")
	lostHash := HashOf(lost)
	b.known.Add(lostHash)
	if _, err = a.db.Exec(`INSERT INTO `+schema+`.blobs (hash, size, stored_size, media_type, kind, created_at)
		VALUES ($1, 1, 1, '', 'test', now() - interval '2 hours')`, lostHash); err != nil {
		t.Fatal(err)
	}
	b.lastSweep = time.Time{}
	b.uploadPending()
	if n := 0; a.db.QueryRow(`SELECT count(*) FROM `+schema+`.blobs WHERE hash = $1`, lostHash).Scan(&n) != nil || n != 0 {
		t.Fatalf("unbacked row not swept (%d)", n)
	}
	if _, err = b.Put(ctx, lost, "", "test"); err != nil {
		t.Fatal(err)
	}
	b.uploadPending()
	if got, err = b.Get(ctx, lostHash); err != nil || string(got) != string(lost) {
		t.Fatalf("re-stored blob = %q %v", got, err)
	}
}

// The request log's pricing pass is a bulk update: it journals one full reconcile, not an entry
// per row, and analytics still ends up with the prices.
func TestPricingPassJournalsOneReconcile(t *testing.T) {
	a, an := openTestAnalytics(t)
	rl := requestlog.Default()
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		if _, err := rl.DB().Exec(`INSERT INTO tmx.requests (ts, outcome, provider, model, uncached_input, total_tokens)
			VALUES (now(), 'success', 'anthropic', 'claude-opus-5-5', 1000000, 1000000)`); err != nil {
			t.Fatal(err)
		}
	}
	drain(t, an)
	if n, err := rl.PricePending(ctx, false); err != nil || n != 5 {
		t.Fatalf("priced %d (%v)", n, err)
	}
	var perRow, reconcile int
	_ = a.db.QueryRow(`SELECT count(*) FILTER (WHERE kind = 'request'), count(*) FILTER (WHERE kind = 'requests_truncate')
		FROM `+journalTable).Scan(&perRow, &reconcile)
	if perRow != 0 || reconcile != 1 {
		t.Fatalf("journal: %d request entries, %d reconciles", perRow, reconcile)
	}
	drain(t, an)
	if n := count(t, an, `SELECT count(*) FROM requests WHERE cost_usd = 4`); n != 5 {
		t.Fatalf("analytics priced rows = %d", n)
	}
}

// A session that failed before (e.g. a blob was missing) and is now extracted loses its old
// failure rows.
func TestRecoveredSessionClearsFailures(t *testing.T) {
	a, an := openTestAnalytics(t)
	ctx := context.Background()
	ts := time.Date(2026, 10, 21, 9, 0, 0, 0, time.UTC)
	if _, err := a.IngestSession(ctx, SessionIngest{Harness: "opencode", ExternalID: "was-broken", StartedAt: &ts,
		Messages: []MessageIngest{{ExternalID: "m", Seq: 1, Role: "user", TS: &ts, Message: json.RawMessage(`{"role":"user","content":"hi"}`)}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := an.db.Exec(`INSERT INTO failed (journal_id, kind, ref, error) VALUES (-1, 'session', 'opencode:was-broken', 'key missing'),
		(-2, 'session', 'opencode:other', 'still broken')`); err != nil {
		t.Fatal(err)
	}
	drain(t, an)
	if n := count(t, an, `SELECT count(*) FROM failed WHERE ref = 'opencode:was-broken'`); n != 0 {
		t.Fatalf("stale failures left: %d", n)
	}
	if n := count(t, an, `SELECT count(*) FROM failed`); n != 1 {
		t.Fatalf("other failures = %d", n)
	}
}
