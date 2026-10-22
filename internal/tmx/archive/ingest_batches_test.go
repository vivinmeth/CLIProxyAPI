package archive

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"strings"
	"testing"
	"time"
)

// Prompt history, attachments and appended session files. Need TMX_REQUESTLOG_TEST_DSN and
// TMX_ANALYTICS_TEST_DSN (see analytics_test.go).

func openTestIngest(t *testing.T) (*Archive, *analytics) {
	t.Helper()
	a, an := openTestAnalytics(t)
	for _, stmt := range []string{"TRUNCATE prompt_history", "TRUNCATE session_attachments"} {
		if _, err := an.db.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}
	return a, an
}

func strp(s string) *string { return &s }

func statuses(res *BatchResult) []string {
	out := make([]string, len(res.Results))
	for i, r := range res.Results {
		out[i] = r.Status
	}
	return out
}

func wantStatuses(t *testing.T, res *BatchResult, want ...string) {
	t.Helper()
	got := statuses(res)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("statuses = %v, want %v (results %+v)", got, want, res.Results)
	}
	for i, r := range res.Results {
		if r.Index != i {
			t.Fatalf("result %d has index %d", i, r.Index)
		}
	}
}

func TestPromptHistoryIngest(t *testing.T) {
	a, an := openTestIngest(t)
	ctx := context.Background()
	claude1 := `{"display":"fix the flaky test","pastedContents":{},"timestamp":1759046400123,"project":"/Users/v/repos/fleet","sessionId":"5d0c6a1e-0000-4000-8000-000000000001"}`
	claude2 := `{"display":"now ship it","pastedContents":{"1":{"id":1,"type":"text","content":"log"}},"timestamp":1759046500000,"project":"/Users/v/repos/fleet"}`
	codex1 := `{"session_id":"019a0000-0000-7000-8000-000000000002","ts":1759046600,"text":"explain the diff"}`
	notJSON := "garbage \xff line"
	batch := PromptBatch{Machine: "sonofanton", Harness: "claude-code", File: "~/.claude/history.jsonl", Lines: []PromptLine{
		{Raw: strp(claude1), LineNo: 1},
		{Raw: strp(claude2), LineNo: 2},
		{RawBase64: base64.StdEncoding.EncodeToString([]byte(notJSON)), LineNo: 3}, // stored raw, with a warning
		{Raw: strp("two\nlines"), LineNo: 4},                                       // error
		{Raw: strp(""), LineNo: 5},                                                 // error
		{Raw: strp(claude1), RawBase64: "eA==", LineNo: 6},                         // error
	}}
	res, err := a.IngestPrompts(ctx, batch)
	if err != nil {
		t.Fatal(err)
	}
	wantStatuses(t, res, "created", "created", "created", "error", "error", "error")
	if res.Created != 3 || res.Errors != 3 || res.HTTPStatus() != 207 {
		t.Fatalf("summary %+v status %d", res, res.HTTPStatus())
	}
	if res.Results[2].Warning == "" || res.Results[3].Code != 400 {
		t.Fatalf("results %+v", res.Results)
	}
	// The raw line is kept byte for byte (here: not even UTF-8).
	raw, err := a.Blobs().Get(ctx, res.Results[2].LineHash)
	if err != nil || string(raw) != notJSON {
		t.Fatalf("raw line = %q, %v", raw, err)
	}

	// Re-sending the file is harmless: same lines -> exists, same ids.
	again, err := a.IngestPrompts(ctx, PromptBatch{Machine: "sonofanton", Harness: "claude-code", Lines: batch.Lines[:3]})
	if err != nil {
		t.Fatal(err)
	}
	wantStatuses(t, again, "exists", "exists", "exists")
	if again.HTTPStatus() != 200 || again.Results[0].ID != res.Results[0].ID {
		t.Fatalf("re-send %+v", again)
	}
	// Dedup is per machine and harness.
	other, err := a.IngestPrompts(ctx, PromptBatch{Machine: "alien", Harness: "claude-code", Lines: batch.Lines[:1]})
	if err != nil {
		t.Fatal(err)
	}
	wantStatuses(t, other, "created")
	codex, err := a.IngestPrompts(ctx, PromptBatch{Machine: "sonofanton", Harness: "codex", Lines: []PromptLine{{Raw: strp(codex1), LineNo: 1}}})
	if err != nil {
		t.Fatal(err)
	}
	wantStatuses(t, codex, "created")
	if _, err = a.IngestPrompts(ctx, PromptBatch{Machine: "sonofanton", Harness: "claude code"}); err == nil {
		t.Fatal("harness with a space accepted")
	}

	// Parsed index.
	items, next, err := a.ListPrompts(ctx, PromptQuery{Machine: "sonofanton", Harness: "claude-code"}, true)
	if err != nil || next != "" || len(items) != 3 {
		t.Fatalf("list = %d items, next %q, %v", len(items), next, err)
	}
	first := items[1] // newest first; the unparsed line (no ts) sorts last
	if first.TS == nil || !first.TS.Equal(time.UnixMilli(1759046400123)) || first.CWD != "/Users/v/repos/fleet" ||
		first.Project != "fleet" || first.SessionKey != "claude-code:5d0c6a1e-0000-4000-8000-000000000001" ||
		first.Text == nil || *first.Text != "fix the flaky test" || first.TextHash != HashOf([]byte("fix the flaky test")) {
		t.Fatalf("parsed claude line %+v", first)
	}
	if items[2].ParseWarning == "" || items[2].TS != nil {
		t.Fatalf("unparsed line %+v", items[2])
	}
	bySession, _, err := a.ListPrompts(ctx, PromptQuery{SessionID: "019a0000-0000-7000-8000-000000000002"}, false)
	if err != nil || len(bySession) != 1 || bySession[0].Harness != "codex" || !bySession[0].TS.Equal(time.Unix(1759046600, 0)) {
		t.Fatalf("codex by session = %+v, %v", bySession, err)
	}
	page1, cur, err := a.ListPrompts(ctx, PromptQuery{Limit: 2}, false)
	if err != nil || len(page1) != 2 || cur == "" {
		t.Fatalf("page 1 = %d, %q, %v", len(page1), cur, err)
	}
	page2, cur2, err := a.ListPrompts(ctx, PromptQuery{Limit: 2, Cursor: cur}, false)
	if err != nil || len(page2) != 2 || cur2 == "" || page2[0].ID == page1[1].ID {
		t.Fatalf("page 2 = %d, %q, %v", len(page2), cur2, err)
	}
	page3, cur3, err := a.ListPrompts(ctx, PromptQuery{Limit: 2, Cursor: cur2}, false)
	if err != nil || len(page3) != 1 || cur3 != "" {
		t.Fatalf("page 3 = %d, %q, %v", len(page3), cur3, err)
	}

	// Analytics gets each line once, with its text.
	drain(t, an)
	if n := count(t, an, `SELECT count(*) FROM prompt_history`); n != 5 {
		t.Fatalf("analytics prompts = %d", n)
	}
	if n := count(t, an, `SELECT count(*) FROM prompt_history WHERE session_key = 'codex:019a0000-0000-7000-8000-000000000002'
		AND text = 'explain the diff'`); n != 1 {
		t.Fatal("codex prompt not extracted")
	}
	// Backfill re-extracts every line (idempotent).
	if _, err = an.db.Exec(`TRUNCATE prompt_history`); err != nil {
		t.Fatal(err)
	}
	bf, err := a.AnalyticsBackfill(ctx)
	if err != nil || bf.Prompts != 5 {
		t.Fatalf("backfill %+v, %v", bf, err)
	}
	drain(t, an)
	if n := count(t, an, `SELECT count(*) FROM prompt_history`); n != 5 {
		t.Fatalf("analytics prompts after backfill = %d", n)
	}
}

func TestAttachmentsIngest(t *testing.T) {
	a, an := openTestIngest(t)
	ctx := context.Background()
	toolOut := []byte("full tool output\n" + strings.Repeat("x", 5000))
	png := append([]byte("\x89PNG\r\n\x1a\n"), bytes.Repeat([]byte{3}, 300)...)
	opencode := []byte("opencode tool output")
	mtime := time.Date(2026, 10, 22, 4, 0, 0, 0, time.UTC)
	b64 := base64.StdEncoding.EncodeToString
	batch := AttachmentBatch{Machine: "alien", Attachments: []AttachmentIngest{
		{Harness: "claude-code", SessionID: "sess-1", Path: "tool-results/bauj7n7sq.txt", ContentType: "text/plain",
			Size: int64(len(toolOut)), SHA256: HashOf(toolOut), MTime: &mtime, DataBase64: b64(toolOut)},
		{Harness: "claude-code", SessionID: "sess-1", Path: "images/1.png", ContentType: "image/png",
			Size: int64(len(png)), SHA256: HashOf(png)}, // not uploaded yet
		{Harness: "opencode", Path: "tool-output/tool_01.txt", ExternalPath: "/home/v/.local/share/opencode/tool-output/tool_01.txt",
			Size: int64(len(opencode)), SHA256: HashOf(opencode), DataBase64: b64(opencode)},
		{Harness: "claude-code", SessionID: "sess-1", Path: "tool-results/bad.txt", Size: 3, SHA256: HashOf([]byte("abd")), DataBase64: b64([]byte("abc"))},
		{Harness: "claude-code", SessionID: "sess-1", Path: "/abs/path", Size: 3, SHA256: HashOf([]byte("abc"))},
		{Harness: "claude-code", SessionID: "sess-1", Path: "tool-results/empty.txt", Size: 0, SHA256: HashOf(nil)},
	}}
	res, err := a.IngestAttachments(ctx, batch)
	if err != nil {
		t.Fatal(err)
	}
	wantStatuses(t, res, "created", "missing_blob", "created", "error", "error", "created")
	if res.Results[1].Code != 409 || res.Results[3].Code != 422 || res.Results[4].Code != 400 || res.HTTPStatus() != 207 {
		t.Fatalf("results %+v", res.Results)
	}
	// Upload the image, re-send everything: the first two are known, the image is now stored.
	if _, err = a.Blobs().Put(ctx, png, "image/png", "import.attachment"); err != nil {
		t.Fatal(err)
	}
	again, err := a.IngestAttachments(ctx, AttachmentBatch{Machine: "alien", Attachments: batch.Attachments[:3]})
	if err != nil {
		t.Fatal(err)
	}
	wantStatuses(t, again, "exists", "created", "exists")
	if again.HTTPStatus() != 200 || again.Results[0].ID != res.Results[0].ID {
		t.Fatalf("re-send %+v", again)
	}
	// Size must match the stored blob.
	wrong := batch.Attachments[1]
	wrong.Size++
	bad, err := a.IngestAttachments(ctx, AttachmentBatch{Attachments: []AttachmentIngest{wrong}})
	if err != nil || bad.Results[0].Code != 422 {
		t.Fatalf("size mismatch %+v, %v", bad, err)
	}
	// A changed file at the same path is a second version; the first stays.
	changed := []byte("tool output, rewritten")
	v2, err := a.IngestAttachments(ctx, AttachmentBatch{Attachments: []AttachmentIngest{{Harness: "claude-code",
		SessionID: "sess-1", Path: "tool-results/bauj7n7sq.txt", Size: int64(len(changed)), SHA256: HashOf(changed),
		DataBase64: b64(changed)}}})
	if err != nil {
		t.Fatal(err)
	}
	wantStatuses(t, v2, "created")

	list, next, err := a.ListAttachments(ctx, AttachmentQuery{SessionID: "claude-code:sess-1"})
	if err != nil || next != "" || len(list) != 4 {
		t.Fatalf("session list = %+v, %q, %v", list, next, err)
	}
	byPath, _, err := a.ListAttachments(ctx, AttachmentQuery{Harness: "claude-code", SessionID: "sess-1", Path: "tool-results/bauj7n7sq.txt"})
	if err != nil || len(byPath) != 2 {
		t.Fatalf("by path = %d, %v", len(byPath), err)
	}
	oc, _, err := a.ListAttachments(ctx, AttachmentQuery{Harness: "opencode", Machine: "alien"})
	if err != nil || len(oc) != 1 || oc[0].ExternalPath == "" || oc[0].SessionKey != "" {
		t.Fatalf("opencode = %+v, %v", oc, err)
	}
	data, err := a.Blobs().Get(ctx, byPath[1].SHA256)
	if err != nil || !bytes.Equal(data, toolOut) {
		t.Fatalf("content: %v", err)
	}
	if mt := a.Blobs().MediaType(ctx, HashOf(png)); mt != "image/png" {
		t.Fatalf("media type %q", mt)
	}
	drain(t, an)
	if n := count(t, an, `SELECT count(*) FROM session_attachments WHERE session_key = 'claude-code:sess-1'`); n != 4 {
		t.Fatalf("analytics attachments = %d", n)
	}
}

func sha(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

func readFile(t *testing.T, a *Archive, ref FileRef, gen int) ([]byte, *FileState) {
	t.Helper()
	var buf bytes.Buffer
	var st *FileState
	if err := a.WriteFile(context.Background(), ref, gen, func(s *FileState) { st = s }, &buf); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes(), st
}

func TestSessionFileAppend(t *testing.T) {
	a, an := openTestIngest(t)
	ctx := context.Background()
	b64 := base64.StdEncoding.EncodeToString
	name := "81ed1be1.jsonl"
	ref := FileRef{Harness: "claude-code", SessionID: "81ed1be1", Name: name}
	v1 := []byte(`{"type":"user","n":1}` + "\n" + `{"type":"assistant","n":2}` + "\n")

	// The first upload is whole, as today: blob + session with the file as a source.
	if _, err := a.Blobs().Put(ctx, v1, "application/x-ndjson", "import.source"); err != nil {
		t.Fatal(err)
	}
	if _, err := a.IngestSession(ctx, SessionIngest{Harness: "claude-code", ExternalID: "81ed1be1",
		Messages: []MessageIngest{{ExternalID: "m1", Seq: 1, Role: "user", Message: []byte(`{"role":"user","content":"hi"}`)}},
		Sources:  []SourceIngest{{Name: name, Blob: HashOf(v1)}}}); err != nil {
		t.Fatal(err)
	}
	st, err := a.FileStateOf(ctx, ref)
	if err != nil || st.Length != int64(len(v1)) || st.SHA256 != HashOf(v1) || st.Origin != "source" || st.Generation != 1 {
		t.Fatalf("state after full upload %+v, %v", st, err)
	}
	if _, err = a.FileStateOf(ctx, FileRef{Harness: "claude-code", SessionID: "81ed1be1", Name: "other.jsonl"}); err != ErrFileNotFound {
		t.Fatalf("unknown file: %v", err)
	}
	if _, err = a.db.Exec(`DELETE FROM ` + journalTable); err != nil {
		t.Fatal(err)
	}

	// The file grew: send only the new bytes.
	add1 := []byte(`{"type":"user","n":3}` + "\n")
	v2 := append(append([]byte{}, v1...), add1...)
	res, err := a.AppendFiles(ctx, FileAppendBatch{Appends: []FileAppend{{Harness: "claude-code", SessionID: "81ed1be1",
		Name: name, Offset: int64(len(v1)), BaseSHA256: HashOf(v1), DataBase64: b64(add1), SHA256: sha(v2)}}})
	if err != nil {
		t.Fatal(err)
	}
	wantStatuses(t, res, "appended")
	if f := res.Results[0].File; f == nil || f.Length != int64(len(v2)) || f.SHA256 != sha(v2) || f.Chunks != 2 || res.HTTPStatus() != 200 {
		t.Fatalf("append result %+v", res.Results[0])
	}
	var journaled int
	if err = a.db.QueryRow(`SELECT count(*) FROM ` + journalTable + ` WHERE kind = 'session' AND ref = 'claude-code:81ed1be1'`).Scan(&journaled); err != nil || journaled != 1 {
		t.Fatalf("append journaled %d session entries, %v", journaled, err)
	}
	got, gst := readFile(t, a, ref, 0)
	if !bytes.Equal(got, v2) || gst.SHA256 != sha(v2) || gst.Chunks != 2 {
		t.Fatalf("content after append = %q (%+v)", got, gst)
	}

	// Re-sending the same append (response lost) is "exists", nothing changes.
	res, err = a.AppendFiles(ctx, FileAppendBatch{Appends: []FileAppend{{Harness: "claude-code", SessionID: "81ed1be1",
		Name: name, Offset: int64(len(v1)), DataBase64: b64(add1)}}})
	if err != nil {
		t.Fatal(err)
	}
	wantStatuses(t, res, "exists")
	if res.Results[0].File.Length != int64(len(v2)) {
		t.Fatalf("exists state %+v", res.Results[0].File)
	}

	// A mixed batch: the next append, a stale offset, a wrong base hash, a wrong final hash,
	// a new file, a new file at a non-zero offset, a blob that isn't uploaded.
	add2 := []byte(`{"type":"assistant","n":4}` + "\n")
	v3 := append(append([]byte{}, v2...), add2...)
	notUploaded := []byte("later")
	fresh := []byte("first bytes of a subagent file\n")
	res, err = a.AppendFiles(ctx, FileAppendBatch{Appends: []FileAppend{
		{Harness: "claude-code", SessionID: "81ed1be1", Name: name, Offset: int64(len(v2)), DataBase64: b64(add2)},
		{Harness: "claude-code", SessionID: "81ed1be1", Name: name, Offset: int64(len(v1)), DataBase64: b64([]byte("stale\n"))},
		{Harness: "claude-code", SessionID: "81ed1be1", Name: name, Offset: int64(len(v3)), BaseSHA256: sha(v2), DataBase64: b64([]byte("x"))},
		{Harness: "claude-code", SessionID: "81ed1be1", Name: name, Offset: int64(len(v3)), SHA256: sha([]byte("nope")), DataBase64: b64([]byte("y"))},
		{Harness: "claude-code", SessionID: "81ed1be1", Name: "subagents/agent-a1.jsonl", Offset: 0, DataBase64: b64(fresh)},
		{Harness: "claude-code", SessionID: "81ed1be1", Name: "subagents/agent-a2.jsonl", Offset: 10, DataBase64: b64(fresh)},
		{Harness: "claude-code", SessionID: "81ed1be1", Name: name, Offset: int64(len(v3)), Blob: HashOf(notUploaded)},
		{Harness: "claude-code", SessionID: "81ed1be1", Name: name, Offset: int64(len(v3))},
	}})
	if err != nil {
		t.Fatal(err)
	}
	wantStatuses(t, res, "appended", "conflict", "conflict", "error", "created", "conflict", "missing_blob", "error")
	if c := res.Results[1]; c.Code != 409 || c.File == nil || c.File.Length != int64(len(v3)) || c.File.SHA256 != sha(v3) {
		t.Fatalf("conflict must carry the server's state: %+v", c)
	}
	if c := res.Results[5]; c.File == nil || c.File.Length != 0 {
		t.Fatalf("conflict on an unknown file: %+v", c)
	}
	if res.Results[3].Code != 422 || res.HTTPStatus() != 207 {
		t.Fatalf("results %+v", res.Results)
	}
	if got, _ = readFile(t, a, ref, 0); !bytes.Equal(got, v3) {
		t.Fatalf("a failed item changed the file: %q", got)
	}
	// Only conflicts -> 409.
	res, err = a.AppendFiles(ctx, FileAppendBatch{Appends: []FileAppend{
		{Harness: "claude-code", SessionID: "81ed1be1", Name: name, Offset: 1, DataBase64: b64([]byte("z"))}}})
	if err != nil || res.HTTPStatus() != 409 {
		t.Fatalf("conflict-only batch %d, %v", res.HTTPStatus(), err)
	}

	// A big append goes as a blob.
	add3 := bytes.Repeat([]byte(`{"type":"tool","n":5}`+"\n"), 1000)
	v4 := append(append([]byte{}, v3...), add3...)
	if _, err = a.Blobs().Put(ctx, add3, "application/octet-stream", "import.source.chunk"); err != nil {
		t.Fatal(err)
	}
	res, err = a.AppendFiles(ctx, FileAppendBatch{Appends: []FileAppend{{Harness: "claude-code", SessionID: "81ed1be1",
		Name: name, Offset: int64(len(v3)), Blob: HashOf(add3), SHA256: sha(v4)}}})
	if err != nil {
		t.Fatal(err)
	}
	wantStatuses(t, res, "appended")

	// A whole upload again (the fallback after a conflict) starts generation 2 from that blob;
	// appends then continue on it, and generation 1 stays readable.
	v5 := append(append([]byte{}, v4...), []byte(`{"type":"user","n":6}`+"\n")...)
	if _, err = a.Blobs().Put(ctx, v5, "application/x-ndjson", "import.source"); err != nil {
		t.Fatal(err)
	}
	if _, err = a.IngestSession(ctx, SessionIngest{Harness: "claude-code", ExternalID: "81ed1be1",
		Sources: []SourceIngest{{Name: name, Blob: HashOf(v5)}}}); err != nil {
		t.Fatal(err)
	}
	st, err = a.FileStateOf(ctx, ref)
	if err != nil || st.Origin != "source" || st.Generation != 2 || st.SHA256 != sha(v5) {
		t.Fatalf("state after re-upload %+v, %v", st, err)
	}
	add4 := []byte(`{"type":"assistant","n":7}` + "\n")
	v6 := append(append([]byte{}, v5...), add4...)
	res, err = a.AppendFiles(ctx, FileAppendBatch{Appends: []FileAppend{{Harness: "claude-code", SessionID: "81ed1be1",
		Name: name, Offset: int64(len(v5)), BaseSHA256: sha(v5), DataBase64: b64(add4)}}})
	if err != nil {
		t.Fatal(err)
	}
	wantStatuses(t, res, "appended")
	if got, gst = readFile(t, a, ref, 0); !bytes.Equal(got, v6) || gst.Generation != 2 || gst.Chunks != 2 {
		t.Fatalf("generation 2 = %d bytes (%+v)", len(got), gst)
	}
	if got, _ = readFile(t, a, ref, 1); !bytes.Equal(got, v4) {
		t.Fatal("generation 1 changed")
	}

	// Reset: the client's bytes become the file (generation 3); re-sending it is "exists".
	short := []byte(`{"type":"user","n":1}` + "\n")
	for i, want := range []string{"created", "exists"} {
		res, err = a.AppendFiles(ctx, FileAppendBatch{Appends: []FileAppend{{Harness: "claude-code", SessionID: "81ed1be1",
			Name: name, Reset: true, DataBase64: b64(short)}}})
		if err != nil {
			t.Fatal(err)
		}
		wantStatuses(t, res, want)
		if res.Results[0].File.Generation != 3 {
			t.Fatalf("reset %d: %+v", i, res.Results[0].File)
		}
	}

	// The session lists its files.
	sess, err := a.GetSession(ctx, "claude-code:81ed1be1")
	if err != nil {
		t.Fatal(err)
	}
	files := sess["files"].([]FileState)
	if len(files) != 2 || files[0].Name != name || files[0].Length != int64(len(short)) || files[1].Name != "subagents/agent-a1.jsonl" {
		t.Fatalf("session files %+v", files)
	}
	batch, err := a.FileStates(ctx, []FileRef{ref, {Harness: "claude-code", SessionID: "81ed1be1", Name: "nope"}, {Name: "x"}})
	if err != nil {
		t.Fatal(err)
	}
	wantStatuses(t, batch, "found", "not_found", "error")
	// Analytics still extracts the session (messages come from the session upsert).
	drain(t, an)
	if n := count(t, an, `SELECT count(*) FROM sessions WHERE id = 'claude-code:81ed1be1'`); n != 1 {
		t.Fatal("session not extracted")
	}
}
