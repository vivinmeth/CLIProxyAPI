package archive

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestRebuildUsageRestoresOnlyMissingRows(t *testing.T) {
	a, rl := openTestArchive(t)
	ctx := context.Background()
	ts := time.Now().UTC().AddDate(-1, -2, 0)
	usage := json.RawMessage(`{"provider":"cursor","model":"auto","uncached_input":100,"output":20}`)
	in := SessionIngest{Harness: "cursor", ExternalID: "c-1", Machine: "sonofanton", Messages: []MessageIngest{
		{ExternalID: "m1", Seq: 1, Role: "assistant", TS: &ts, Message: json.RawMessage(`{"a":1}`), Usage: usage},
		{ExternalID: "m2", Seq: 2, Role: "assistant", TS: &ts, Message: json.RawMessage(`{"a":2}`), Usage: usage},
		{ExternalID: "m3", Seq: 3, Role: "user", TS: &ts, Message: json.RawMessage(`{"a":3}`)},
	}}
	if _, err := a.IngestSession(ctx, in); err != nil {
		t.Fatal(err)
	}
	// Simulate the old retention purge of one row, and mark the survivor so we can tell it was
	// not rewritten.
	if _, err := rl.DB().Exec(`DELETE FROM tmx.requests WHERE import_ref = 'cursor:c-1#m1'`); err != nil {
		t.Fatal(err)
	}
	if _, err := rl.DB().Exec(`UPDATE tmx.requests SET error = 'untouched' WHERE import_ref = 'cursor:c-1#m2'`); err != nil {
		t.Fatal(err)
	}
	dry, err := a.RebuildUsage(ctx, "", true)
	if err != nil || dry.Missing != 1 || dry.Written != 0 {
		t.Fatalf("dry run = %+v %v", dry, err)
	}
	res, err := a.RebuildUsage(ctx, "cursor", false)
	if err != nil || res.Missing != 1 || res.Written != 1 || res.Sessions != 1 {
		t.Fatalf("rebuild = %+v %v", res, err)
	}
	var n int
	var machine, harness string
	var restoredTS time.Time
	if err = rl.DB().QueryRow(`SELECT machine, harness, ts FROM tmx.requests WHERE import_ref = 'cursor:c-1#m1'`).Scan(&machine, &harness, &restoredTS); err != nil {
		t.Fatal(err)
	}
	if machine != "sonofanton" || harness != "cursor" || !restoredTS.Equal(ts.Truncate(time.Microsecond)) {
		t.Fatalf("restored row: machine=%q harness=%q ts=%v", machine, harness, restoredTS)
	}
	var errText string
	_ = rl.DB().QueryRow(`SELECT error FROM tmx.requests WHERE import_ref = 'cursor:c-1#m2'`).Scan(&errText)
	if errText != "untouched" {
		t.Fatal("existing row was rewritten")
	}
	_ = rl.DB().QueryRow(`SELECT count(*) FROM tmx.requests WHERE starts_with(import_ref, 'cursor:c-1#')`).Scan(&n)
	if n != 2 {
		t.Fatalf("rows = %d, want 2", n)
	}
	again, err := a.RebuildUsage(ctx, "", false)
	if err != nil || again.Missing != 0 {
		t.Fatalf("second run = %+v %v", again, err)
	}
}
