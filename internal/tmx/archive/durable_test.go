package archive

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/capture"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/requestlog"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/spool/spooltest"
)

func spoolTestExchange(trace string, status int, big []byte) *capture.Exchange {
	start := time.Now().Add(-2 * time.Second).UTC().Truncate(time.Millisecond)
	ex := &capture.Exchange{
		TraceID: trace, URL: "/v1/messages", Method: "POST", StartedAt: start, FinishedAt: start.Add(time.Second),
		Status: status, RequestModel: "claude-opus-5-5",
		RequestHeaders:  map[string][]string{"Authorization": {"Bearer sk-secret-" + trace}, "User-Agent": {"claude-cli/2"}},
		ResponseHeaders: map[string][]string{"Content-Type": {"text/plain"}},
		Bodies: map[string]*capture.Body{
			"request":  capture.Capture([]byte(`{"model":"claude-opus-5-5","messages":[{"role":"user","content":"hi `+trace+`"}]}`), nil),
			"response": capture.Capture(big, nil),
		},
		Errors: []string{},
	}
	return ex
}

// An exchange survives the spool byte for byte: spilled and in-memory bodies, metadata, and the
// rejected row (computed before credentials are redacted; no credential reaches the disk).
func TestSpooledExchangeRoundTrip(t *testing.T) {
	dir := t.TempDir()
	capture.Configure(0, 1024, dir) // bodies over 1 KiB spill to disk
	defer capture.Configure(0, 0, "")
	big := bytes.Repeat([]byte("0123456789"), 1000)
	ex := spoolTestExchange("rt-1", 401, big)
	if !ex.Bodies["response"].Spilled() {
		t.Fatal("expected a spilled body")
	}
	defer ex.Release()
	var buf bytes.Buffer
	if err := encodeExchange(&buf, ex, "test"); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(buf.String(), "sk-secret") {
		t.Fatal("credential written to the spool")
	}
	out, rej, err := decodeExchange(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	r, _ := out.Bodies["response"].Reader()
	got, _ := io.ReadAll(r)
	if !bytes.Equal(got, big) || out.TraceID != "rt-1" || !out.StartedAt.Equal(ex.StartedAt) || out.Status != 401 {
		t.Fatalf("decoded %q… trace %s ts %v", got[:min(20, len(got))], out.TraceID, out.StartedAt)
	}
	want, _ := rejectedRow(ex)
	if rej == nil || rej.APIKeyHash == "" || rej.APIKeyHash != want.APIKeyHash || rej.Outcome != requestlog.OutcomeRejected {
		t.Fatalf("rejected row = %+v", rej)
	}
	// Truncated bytes are a permanent decode error.
	if _, _, err = decodeExchange(bytes.NewReader(buf.Bytes()[:buf.Len()-10])); err == nil {
		t.Fatal("truncated exchange decoded")
	}
}

// A database outage loses no exchange: failed stores and overflow are spooled and stored once the
// database is back.
func TestArchiveOutageLosesNothing(t *testing.T) {
	dsn := os.Getenv("TMX_REQUESTLOG_TEST_DSN")
	if dsn == "" {
		t.Skip("TMX_REQUESTLOG_TEST_DSN not set")
	}
	t.Setenv("TMX_SPOOL_DIR", "")
	proxied, proxy := spooltest.DSN(t, dsn)
	blobDir := t.TempDir()
	a, err := Open(context.Background(), proxied, nil, "unused", "dev", blobDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { proxy.Up(); a.Close() })
	if a.spool == nil || !strings.HasPrefix(a.spool.Dir(), filepath.Join(blobDir, "durable")) {
		t.Fatalf("spool not under the blob spool: %v", a.spool)
	}
	prefix := fmt.Sprintf("dur-%d-", time.Now().UnixNano())
	big := bytes.Repeat([]byte("abc"), 5000)

	proxy.Down()
	for i := 0; i < 10; i++ {
		a.enqueue(spoolTestExchange(fmt.Sprintf("%s%02d", prefix, i), 200, big))
	}
	a.overflow(spoolTestExchange(prefix+"overflow", 200, big))
	if err = a.FlushContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	if d := a.Durability(); d.SpooledPending != 11 || d.Dropped != 0 {
		t.Fatalf("while down: %+v", d)
	}
	raw, _ := os.ReadFile(firstSegment(t, a.spool.Dir()))
	if bytes.Contains(raw, []byte("sk-secret")) {
		t.Fatal("credential written to the spool")
	}

	proxy.Up()
	deadline := time.Now().Add(60 * time.Second)
	for a.spool.Pending() > 0 {
		if time.Now().After(deadline) {
			t.Fatalf("not replayed: %+v", a.Durability())
		}
		time.Sleep(50 * time.Millisecond)
	}
	var n int
	if err = a.db.QueryRow(`SELECT count(*) FROM `+schema+`.exchanges WHERE trace_id LIKE $1`, prefix+"%").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 11 {
		t.Fatalf("exchanges stored = %d", n)
	}
	out, err := a.GetExchange(context.Background(), prefix+"07")
	if err != nil {
		t.Fatal(err)
	}
	if out["response_body"] != string(big) {
		t.Fatalf("response body not stored intact")
	}
	if d := a.Durability(); d.Replayed != 11 || d.Dropped != 0 || d.Deadlettered != 0 {
		t.Fatalf("after: %+v", d)
	}
}

func firstSegment(t *testing.T, dir string) string {
	t.Helper()
	m, _ := filepath.Glob(filepath.Join(dir, "seg-*.spool"))
	if len(m) == 0 {
		t.Fatal("no spool segment")
	}
	return m[0]
}
