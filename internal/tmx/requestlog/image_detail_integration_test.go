package requestlog

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	coreusage "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/usage"
)

func TestRowFromRecordKeepsImageDetail(t *testing.T) {
	d := &coreusage.ImageDetail{Operation: "edit", InputImages: 1, ReturnedCount: 1}
	row := rowFromRecord(context.Background(), coreusage.Record{Provider: "codex", Model: "gpt-image-2", Image: d})
	d.ReturnedCount = 5
	if row.ImageDetail == nil || row.ImageDetail.Operation != "edit" || row.ImageDetail.ReturnedCount != 1 {
		t.Fatalf("row image detail = %+v", row.ImageDetail)
	}
	if rowFromRecord(context.Background(), coreusage.Record{Provider: "claude"}).ImageDetail != nil {
		t.Fatal("a non-image record must have no image detail")
	}
}

// Runs against a real Postgres when TMX_REQUESTLOG_TEST_DSN is set.
func TestImageDetailColumnRoundTrip(t *testing.T) {
	dsn := os.Getenv("TMX_REQUESTLOG_TEST_DSN")
	if dsn == "" {
		t.Skip("TMX_REQUESTLOG_TEST_DSN not set")
	}
	ctx := context.Background()
	schema := fmt.Sprintf("tmx_test_img_%d", time.Now().UnixNano())
	store, err := Open(ctx, dsn, schema, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = store.db.Exec("DROP SCHEMA " + schema + " CASCADE")
		store.Close()
	})
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Minute)
	image := &coreusage.ImageDetail{Operation: "generate", RequestedSize: "1024x1024", RequestedQuality: "low",
		RequestedN: 1, ReturnedCount: 1, ReturnedSize: "1536x1024", ReturnedQuality: "medium"}
	if err = store.insert([]Row{
		{TS: base, TraceID: "img", Provider: "codex", Model: "gpt-image-2", Outcome: OutcomeSuccess, RowKey: "k-img", ImageDetail: image},
		{TS: base.Add(time.Second), TraceID: "txt", Provider: "codex", Model: "gpt", Outcome: OutcomeSuccess, RowKey: "k-txt"},
	}); err != nil {
		t.Fatal(err)
	}
	var size, quality, n string
	var nulls int
	if err = store.db.QueryRow(`SELECT image_detail->>'returned_size', image_detail->>'requested_quality', image_detail->>'requested_n',
		(SELECT count(*) FROM `+store.table()+` WHERE image_detail IS NULL) FROM `+store.table()+` WHERE trace_id = 'img'`).
		Scan(&size, &quality, &n, &nulls); err != nil {
		t.Fatal(err)
	}
	if size != "1536x1024" || quality != "low" || n != "1" || nulls != 1 {
		t.Fatalf("stored %s %s %s, null rows %d", size, quality, n, nulls)
	}
	items, err := store.List(ctx, Filter{From: base.Add(-time.Minute), To: base.Add(time.Hour)}, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	found := 0
	for _, it := range items {
		switch it.TraceID {
		case "img":
			if it.ImageDetail == nil || *it.ImageDetail != *image {
				t.Fatalf("listed image detail = %+v", it.ImageDetail)
			}
			found++
		case "txt":
			if it.ImageDetail != nil {
				t.Fatalf("text row has image detail %+v", it.ImageDetail)
			}
			found++
		}
	}
	if found != 2 {
		t.Fatalf("listed %d of 2 rows", found)
	}
}
