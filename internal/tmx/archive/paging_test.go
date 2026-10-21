package archive

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/capture"
	"github.com/tidwall/gjson"
)

func gjsonString(doc []byte, path string) string { return gjson.GetBytes(doc, path).String() }

func testExchange(trace, session, body string) *capture.Exchange {
	at := time.Now().Add(-time.Minute)
	return &capture.Exchange{TraceID: trace, SessionID: session, URL: "/v1/messages", Method: "POST",
		StartedAt: at, FinishedAt: at.Add(time.Second), Status: 200, RequestBody: []byte(body)}
}

func msgs(n int, ts time.Time) []MessageIngest {
	out := make([]MessageIngest, n)
	for i := range out {
		out[i] = MessageIngest{ExternalID: fmt.Sprintf("m%d", i), Seq: int64(i + 1), Role: "user", TS: &ts,
			Message: json.RawMessage(fmt.Sprintf(`{"role":"user","content":"msg %d"}`, i))}
	}
	return out
}

// Long conversations page by item offset on every path: imported and proxied sessions rebuilt
// from the archive, and sessions served from analytics.
func TestConversationPages(t *testing.T) {
	a, _ := openTestArchive(t)
	ctx := context.Background()
	ts := time.Now().UTC().Add(-time.Hour)
	if _, err := a.IngestSession(ctx, SessionIngest{Harness: "opencode", ExternalID: "long", Messages: msgs(5, ts)}); err != nil {
		t.Fatal(err)
	}
	page := func(offset, limit int) *Conversation {
		t.Helper()
		conv, err := a.ConversationPage(ctx, "long", "", "", "imported", ConvPage{Offset: offset, Limit: limit})
		if err != nil {
			t.Fatal(err)
		}
		return conv
	}
	// The default window of a transcript is its start; the total isn't known part way.
	first := page(-1, 2)
	if len(first.Items) != 2 || first.Items[0].Text != "msg 0" || !first.Truncated || first.NextOffset != 2 || first.Total != nil {
		t.Fatalf("first page = %+v", first)
	}
	var texts []string
	for off := 0; ; {
		p := page(off, 2)
		if p.Offset != off {
			t.Fatalf("offset = %d, want %d", p.Offset, off)
		}
		for _, it := range p.Items {
			texts = append(texts, it.Text)
		}
		if p.NextOffset == 0 {
			if p.Total == nil || *p.Total != 5 {
				t.Fatalf("last page total = %v", p.Total)
			}
			break
		}
		off = p.NextOffset
	}
	if len(texts) != 5 || texts[4] != "msg 4" {
		t.Fatalf("paged items = %v", texts)
	}
	if whole := page(-1, 0); whole.Truncated || whole.Total == nil || *whole.Total != 5 || len(whole.Items) != 5 {
		t.Fatalf("whole = %+v", whole)
	}

	// Proxied: the default window is the latest items.
	body := `{"model":"m","messages":[{"role":"user","content":"a"},{"role":"assistant","content":"b"},{"role":"user","content":"c"}]}`
	if err := a.storeExchange(ctx, testExchange("px1", "s-px", body)); err != nil {
		t.Fatal(err)
	}
	tail, err := a.ConversationPage(ctx, "s-px", "", "", "proxy", ConvPage{Offset: -1, Limit: 2})
	if err != nil || len(tail.Items) != 2 || tail.Offset != 1 || tail.Items[1].Text != "c" || tail.NextOffset != 0 ||
		tail.Total == nil || *tail.Total != 3 || !tail.Truncated {
		t.Fatalf("proxy tail = %+v %v", tail, err)
	}
	head, err := a.ConversationPage(ctx, "s-px", "", "", "proxy", ConvPage{Offset: 0, Limit: 2})
	if err != nil || len(head.Items) != 2 || head.Items[0].Text != "a" || head.NextOffset != 2 {
		t.Fatalf("proxy head = %+v %v", head, err)
	}
}

func TestConversationPagesFromAnalytics(t *testing.T) {
	a, an := openTestAnalytics(t)
	ctx := context.Background()
	ts := time.Now().UTC().Add(-time.Hour)
	if _, err := a.IngestSession(ctx, SessionIngest{Harness: "opencode", ExternalID: "long", Messages: msgs(5, ts)}); err != nil {
		t.Fatal(err)
	}
	drain(t, an)
	conv, err := a.ConversationPage(ctx, "long", "", "", "", ConvPage{Offset: 3, Limit: 10})
	if err != nil || conv.Source != "analytics" || len(conv.Items) != 2 || conv.Items[0].Text != "msg 3" ||
		conv.Offset != 3 || conv.Total == nil || *conv.Total != 5 || conv.NextOffset != 0 || !conv.Truncated {
		t.Fatalf("analytics page = %+v %v", conv, err)
	}
	first, err := a.ConversationPage(ctx, "long", "", "", "", ConvPage{Offset: -1, Limit: 2})
	if err != nil || len(first.Items) != 2 || first.NextOffset != 2 || first.Items[0].Text != "msg 0" {
		t.Fatalf("analytics first = %+v %v", first, err)
	}
}

// Commits filter by session (either id form) and thread, and page.
func TestListCommitsFiltersAndPages(t *testing.T) {
	a, _ := openTestArchive(t)
	ctx := context.Background()
	ts := time.Date(2026, 10, 21, 9, 0, 0, 0, time.UTC)
	at := func(m int) *time.Time { x := ts.Add(time.Duration(m) * time.Minute); return &x }
	if _, err := a.IngestCommits(ctx, CommitBatch{Commits: []CommitIngest{
		{Repo: "r", SHA: "aaaaaaa1", CommittedAt: at(1), SessionIDs: []string{"ses_x"}},
		{Repo: "r", SHA: "bbbbbbb2", CommittedAt: at(2), SessionIDs: []string{"claude-code:ses_y"}, ThreadIDs: []string{"t3:T"}},
		{Repo: "r", SHA: "ccccccc3", CommittedAt: at(3)},
		{Repo: "r", SHA: "ddddddd4"}, // no time: listed last
	}}); err != nil {
		t.Fatal(err)
	}
	shas := func(q CommitQuery) []string {
		t.Helper()
		items, _, err := a.ListCommits(ctx, q)
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, it := range items {
			out = append(out, gjsonString(it, "sha"))
		}
		return out
	}
	for q, want := range map[CommitQuery]string{
		{SessionID: "ses_x"}:             "aaaaaaa1",
		{SessionID: "opencode:ses_x"}:    "aaaaaaa1", // archive id matches the bare id sent
		{SessionID: "ses_y"}:             "bbbbbbb2", // bare id matches the archive id sent
		{SessionID: "claude-code:ses_y"}: "bbbbbbb2",
		{ThreadID: "t3:T"}:               "bbbbbbb2",
	} {
		if got := shas(q); len(got) != 1 || got[0] != want {
			t.Fatalf("%+v = %v, want %s", q, got, want)
		}
	}
	if got := shas(CommitQuery{SessionID: "ses"}); len(got) != 0 {
		t.Fatalf("partial id matched %v", got)
	}
	var all []string
	cursor := ""
	for {
		items, next, err := a.ListCommits(ctx, CommitQuery{Limit: 1, Cursor: cursor})
		if err != nil {
			t.Fatal(err)
		}
		for _, it := range items {
			all = append(all, gjsonString(it, "sha"))
		}
		if next == "" {
			break
		}
		cursor = next
	}
	if fmt.Sprint(all) != "[ccccccc3 bbbbbbb2 aaaaaaa1 ddddddd4]" {
		t.Fatalf("paged = %v", all)
	}
	if _, _, err := a.ListCommits(ctx, CommitQuery{Cursor: "x"}); !errors.Is(err, ErrBadCursor) {
		t.Fatalf("bad cursor = %v", err)
	}
}

// Threads page and can carry their sessions.
func TestListThreadsPagesWithSessions(t *testing.T) {
	a, _ := openTestArchive(t)
	ctx := context.Background()
	ts := time.Date(2026, 10, 21, 9, 0, 0, 0, time.UTC)
	var threads []ThreadIngest
	for i := 0; i < 5; i++ {
		up := ts.Add(time.Duration(i) * time.Minute)
		threads = append(threads, ThreadIngest{ExternalID: fmt.Sprintf("T%d", i), UpdatedAt: &up,
			Sessions: []ThreadSession{{Harness: "claude-code", SessionID: fmt.Sprintf("s%d", i)}}})
	}
	if _, err := a.IngestThreads(ctx, ThreadBatch{Source: "t3", Threads: threads}); err != nil {
		t.Fatal(err)
	}
	var ids []string
	cursor := ""
	for {
		items, next, err := a.ListThreads(ctx, ThreadQuery{Limit: 2, Cursor: cursor, WithSessions: true})
		if err != nil {
			t.Fatal(err)
		}
		for _, it := range items {
			ids = append(ids, it.ID)
			if gjsonString(it.Sessions, "0.session_id") != "s"+it.ExternalID[1:] {
				t.Fatalf("%s sessions = %s", it.ID, it.Sessions)
			}
		}
		if next == "" {
			break
		}
		cursor = next
	}
	if fmt.Sprint(ids) != "[t3:T4 t3:T3 t3:T2 t3:T1 t3:T0]" {
		t.Fatalf("paged = %v", ids)
	}
	plain, _, err := a.ListThreads(ctx, ThreadQuery{})
	if err != nil || len(plain) != 5 || plain[0].Sessions != nil {
		t.Fatalf("without sessions = %+v %v", plain, err)
	}
}
