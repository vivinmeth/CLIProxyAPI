package archive

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func countImported(t *testing.T, a *Archive, prefix string) int {
	t.Helper()
	var n int
	if err := a.db.QueryRow(`SELECT count(*) FROM tmx.requests WHERE starts_with(import_ref, $1)`, prefix).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestSessionReplaceAndDelete(t *testing.T) {
	a, _ := openTestArchive(t)
	ctx := context.Background()
	ts := time.Date(2026, 10, 20, 10, 0, 0, 0, time.UTC)
	usage := json.RawMessage(`{"provider":"anthropic","model":"claude-opus-5-5","uncached_input":5,"cache_read":10,"cache_write":1,"output":3}`)
	msg := func(id string, seq int64, withUsage bool) MessageIngest {
		m := MessageIngest{ExternalID: id, Seq: seq, Role: "assistant", TS: &ts, Model: "claude-opus-5-5",
			Message: json.RawMessage(`{"role":"assistant","content":"` + id + `"}`)}
		if withUsage {
			m.Usage = usage
		}
		return m
	}
	in := SessionIngest{Harness: "claude-code", ExternalID: "del-1", Machine: "laptop",
		Messages: []MessageIngest{msg("m1", 1, true), msg("m2", 2, true), msg("bad", 3, true)}}
	if _, err := a.IngestSession(ctx, in); err != nil {
		t.Fatal(err)
	}
	key := SessionKey("claude-code", "del-1")
	if n := countImported(t, a, key+"#"); n != 3 {
		t.Fatalf("usage rows = %d, want 3", n)
	}

	// Without replace, leaving "bad" out changes nothing.
	in.Messages = in.Messages[:2]
	if res, err := a.IngestSession(ctx, in); err != nil || res.Removed != 0 {
		t.Fatalf("plain re-ingest = %+v %v", res, err)
	}
	if n := countImported(t, a, key+"#"); n != 3 {
		t.Fatalf("usage rows after plain ingest = %d, want 3", n)
	}

	// replace: true drops the stale message and its usage row.
	in.Replace = true
	res, err := a.IngestSession(ctx, in)
	if err != nil || res.Removed != 1 || res.UsageRemoved != 1 {
		t.Fatalf("replace = %+v %v", res, err)
	}
	if n := countImported(t, a, key+"#"); n != 2 {
		t.Fatalf("usage rows after replace = %d, want 2", n)
	}

	// A second session whose id shares the prefix must survive the delete.
	other := SessionIngest{Harness: "claude-code", ExternalID: "del-10", Messages: []MessageIngest{msg("x", 1, true)}}
	if _, err = a.IngestSession(ctx, other); err != nil {
		t.Fatal(err)
	}
	del, err := a.DeleteSession(ctx, key)
	if err != nil || del.Messages != 2 || del.UsageRows != 2 {
		t.Fatalf("delete = %+v %v", del, err)
	}
	if _, err = a.GetSession(ctx, key); err == nil {
		t.Fatal("session still readable after delete")
	}
	if n := countImported(t, a, SessionKey("claude-code", "del-10")+"#"); n != 1 {
		t.Fatalf("neighbouring session lost usage rows: %d", n)
	}
	if _, err = a.DeleteSession(ctx, key); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("second delete err = %v", err)
	}
}
