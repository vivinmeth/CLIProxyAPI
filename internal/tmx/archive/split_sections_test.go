package archive

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestSplitSectionsPieces(t *testing.T) {
	dump := []byte("=== API REQUEST 1 ===\nHeaders:\nX: 1\n\nBody:\n{\"a\":\"}{\\\"\",\"b\":[1,{\"c\":2}]}\n\n" +
		"=== API REQUEST 2 ===\nBody:\nnot json\n\nBody:\n{\"z\":1}")
	pieces := splitSections(dump)
	var joined []byte
	jsonCount := 0
	for _, p := range pieces {
		joined = append(joined, p.data...)
		if p.json {
			jsonCount++
		}
	}
	if !bytes.Equal(joined, dump) {
		t.Fatalf("pieces do not reassemble:\n%q", joined)
	}
	if jsonCount != 2 {
		t.Fatalf("json bodies = %d, want 2", jsonCount)
	}
	if splitSections([]byte("plain text, no body")) != nil {
		t.Fatal("expected nil for text without a JSON body")
	}
	if splitSections([]byte("Headers:\n\nBody:\n{\"truncated\":")) != nil {
		t.Fatal("expected nil for a truncated JSON body")
	}
}

// The upstream request is a request-log text dump around the JSON body. Its conversation elements
// must dedupe against the client request instead of storing the whole dump each turn.
func TestUpstreamDumpDedupesAgainstClientBody(t *testing.T) {
	a, _ := openTestArchive(t)
	ctx := context.Background()
	long := strings.Repeat("conversation so far. ", 4000)
	client := `{"model":"claude-opus-5-5","messages":[{"role":"user","content":"` + long + `"},` +
		`{"role":"assistant","content":"ok"},{"role":"user","content":"next"}],"stream":true}`
	if _, err := a.splitBody(ctx, []byte(client), "request"); err != nil {
		t.Fatal(err)
	}
	before := countBlobs(t, a)
	var storedBefore int64
	if err := a.db.QueryRow("SELECT coalesce(sum(size),0) FROM " + schema + ".blobs").Scan(&storedBefore); err != nil {
		t.Fatal(err)
	}
	dump := "=== API REQUEST 1 ===\nTimestamp: 2026-10-20T18:38:04+05:30\nUpstream URL: https://api.anthropic.com/v1/messages\n" +
		"Headers:\nAuthorization: Bearer sk-a...FwAA\n\nBody:\n" + client + "\n\n"
	h, err := a.splitBody(ctx, []byte(dump), "upstream.request")
	if err != nil {
		t.Fatal(err)
	}
	got, err := a.rebuildBody(ctx, h)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != dump {
		t.Fatal("upstream dump not rebuilt byte-for-byte")
	}
	var storedAfter int64
	if err = a.db.QueryRow("SELECT coalesce(sum(size),0) FROM " + schema + ".blobs").Scan(&storedAfter); err != nil {
		t.Fatal(err)
	}
	if added := storedAfter - storedBefore; added > 2048 {
		t.Fatalf("upstream dump added %d bytes (blobs %d -> %d), want only header text + manifests",
			added, before, countBlobs(t, a))
	}
}
