package archive

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

// A bad message is refused alone: the rest of the session is stored, the refused one is listed,
// and replace removes nothing while any message is refused.
func TestSessionIngestRefusesBadMessagesAlone(t *testing.T) {
	a, _ := openTestArchive(t)
	ctx := context.Background()
	ts := time.Now().UTC().Truncate(time.Second)
	msg := func(id string, seq int64) MessageIngest {
		return MessageIngest{ExternalID: id, Seq: seq, Role: "user", TS: &ts, Message: json.RawMessage(`{"role":"user","content":"x"}`)}
	}
	stored := func() int {
		var n int
		if err := a.db.QueryRow(`SELECT count(*) FROM ` + schema + `.session_messages WHERE session_id = 'claude-code:peritem'`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	in := SessionIngest{Harness: "claude-code", ExternalID: "peritem", StartedAt: &ts, Messages: []MessageIngest{
		msg("m1", 1),
		{ExternalID: "m2", Seq: 2, Role: "user", Message: json.RawMessage(`{"broken`)},
		msg("m1", 3), // duplicate id
		{ExternalID: "m4", Seq: 4, Role: "user", Message: json.RawMessage(`{}`), Metadata: json.RawMessage(`{"a":"\u0000"}`)},
		msg("m5", 5),
	}, Sources: []SourceIngest{{Name: "", Blob: "nothash"}}}
	res, err := a.IngestSession(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	if res.Inserted != 2 || res.Errors != 4 || len(res.Rejected) != 4 || stored() != 2 {
		t.Fatalf("result = %+v, stored %d", res, stored())
	}
	want := map[string]int{"source:0": 1, "message:1": 1, "message:2": 1, "message:3": 1}
	for _, r := range res.Rejected {
		key := r.Kind + ":" + string(rune('0'+r.Index))
		if want[key] != 1 || r.Error == "" {
			t.Fatalf("unexpected rejection %+v", r)
		}
		delete(want, key)
	}
	if len(want) != 0 {
		t.Fatalf("missing rejections %v", want)
	}

	// Replace with a refused message keeps everything stored.
	res, err = a.IngestSession(ctx, SessionIngest{Harness: "claude-code", ExternalID: "peritem", Replace: true,
		Messages: []MessageIngest{msg("m5", 5), {ExternalID: "bad", Message: json.RawMessage(`nope`)}}})
	if err != nil || !res.ReplaceSkipped || res.Removed != 0 || stored() != 2 {
		t.Fatalf("replace with a refused message = %+v %v, stored %d", res, err, stored())
	}
	// A clean replace still works.
	res, err = a.IngestSession(ctx, SessionIngest{Harness: "claude-code", ExternalID: "peritem", Replace: true,
		Messages: []MessageIngest{msg("m5", 5)}})
	if err != nil || res.ReplaceSkipped || res.Removed != 1 || res.Errors != 0 || stored() != 1 {
		t.Fatalf("clean replace = %+v %v, stored %d", res, err, stored())
	}
	// A session-level problem is still a bad request for the whole body.
	if _, err = a.IngestSession(ctx, SessionIngest{Harness: "claude-code", ExternalID: ""}); !IsBadRequest(err) {
		t.Fatalf("empty id: %v", err)
	}
}
