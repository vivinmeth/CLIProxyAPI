package archive

import (
	"context"
	"encoding/json"
	"math"
	"testing"
	"time"
)

// Commits join tasks: a thread tree (by thread id, or through a session in it) or a session with
// no thread; task costs add up to total spend; PR sizes are kept; re-sending is a no-op.
func TestCommitsAndTasks(t *testing.T) {
	a, an := openTestAnalytics(t)
	ctx := context.Background()
	ts := time.Date(2026, 10, 21, 9, 0, 0, 0, time.UTC)
	usage := `{"provider":"anthropic","model":"claude-opus-5-5","uncached_input":1000000,"output":0}` // $4
	for _, ext := range []string{"t-main", "t-deleg", "loose"} {
		if _, err := a.IngestSession(ctx, SessionIngest{Harness: "claude-code", ExternalID: ext, StartedAt: &ts,
			Messages: []MessageIngest{{ExternalID: "m", Seq: 1, Role: "assistant", TS: &ts,
				Message: json.RawMessage(`{"role":"assistant","content":"x"}`), Usage: json.RawMessage(usage)}}}); err != nil {
			t.Fatal(err)
		}
	}
	merged := ts.Add(time.Hour)
	if _, err := a.IngestThreads(ctx, ThreadBatch{Source: "t3", Threads: []ThreadIngest{
		{ExternalID: "R", Title: "Ship", Sessions: []ThreadSession{{SessionID: "t-main"}},
			PullRequests: []ThreadPullRequest{{URL: "u", State: "merged", MergedAt: &merged, Additions: 50, Deletions: 5, ChangedFiles: 3}}},
		// The same PR linked to the delegate too: counted once for the task.
		{ExternalID: "D", ParentID: "R", ParentKind: "delegate", Sessions: []ThreadSession{{SessionID: "t-deleg"}},
			PullRequests: []ThreadPullRequest{{URL: "u", State: "merged", MergedAt: &merged, Additions: 50, Deletions: 5, ChangedFiles: 3}}},
		// A parent cycle (bad data) is one task.
		{ExternalID: "C1", ParentID: "C2"}, {ExternalID: "C2", ParentID: "C1"},
	}}); err != nil {
		t.Fatal(err)
	}
	batch := CommitBatch{Machine: "alien", Commits: []CommitIngest{
		{Repo: "github.com/o/r", SHA: "aaaaaaa1", CommittedAt: &ts, Additions: 10, Deletions: 2, Files: 1,
			ThreadIDs: []string{"t3:D"}, Match: "trailer"}, // the delegate's commit lands on the root task
		{Repo: "github.com/o/r", SHA: "bbbbbbb2", CommittedAt: &ts, Additions: 30, Deletions: 0, Files: 2,
			SessionIDs: []string{"loose"}, Match: "time_cwd"},
	}}
	res, err := a.IngestCommits(ctx, batch)
	if err != nil || res.Created != 2 {
		t.Fatalf("ingest = %+v %v", res, err)
	}
	if res, err = a.IngestCommits(ctx, batch); err != nil || res.Unchanged != 2 {
		t.Fatalf("re-ingest = %+v %v", res, err)
	}
	// "dup" exists under two harnesses: a commit naming it bare matches neither.
	for _, h := range []string{"claude-code", "codex"} {
		if _, err = a.IngestSession(ctx, SessionIngest{Harness: h, ExternalID: "dup", StartedAt: &ts,
			Messages: []MessageIngest{{ExternalID: "m", Seq: 1, Role: "user", TS: &ts, Message: json.RawMessage(`{"role":"user","content":"x"}`)}}}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = a.IngestCommits(ctx, CommitBatch{Commits: []CommitIngest{{Repo: "github.com/o/r", SHA: "ccccccc3", Additions: 1,
		SessionIDs: []string{"dup"}}, {Repo: "github.com/o/r", SHA: "ddddddd4", Additions: 1, SessionIDs: []string{"codex:dup"}}}}); err != nil {
		t.Fatal(err)
	}
	if _, err = a.IngestCommits(ctx, CommitBatch{Commits: []CommitIngest{{Repo: "r", SHA: "zz"}}}); err == nil {
		t.Fatal("bad sha accepted")
	}
	drain(t, an)

	type task struct {
		kind              string
		sessions, commits int
		add, del, prAdd   int64
		cost              float64
	}
	got := map[string]task{}
	rows, err := an.db.Query(`SELECT task_id, kind, sessions, commits, additions, deletions, pr_additions, COALESCE(cost_usd, 0) FROM tasks`)
	if err != nil {
		t.Fatal(err)
	}
	total := 0.0
	for rows.Next() {
		var id string
		var x task
		if err = rows.Scan(&id, &x.kind, &x.sessions, &x.commits, &x.add, &x.del, &x.prAdd, &x.cost); err != nil {
			t.Fatal(err)
		}
		got[id] = x
		total += x.cost
	}
	_ = rows.Close()
	root, loose := got["t3:R"], got["session:claude-code:loose"]
	if n := count(t, an, `SELECT count(*) FROM commit_tasks WHERE commit_id LIKE '%@ccccccc3'`); n != 0 {
		t.Fatalf("ambiguous external id matched %d tasks", n)
	}
	if n := count(t, an, `SELECT count(*) FROM commit_tasks WHERE commit_id LIKE '%@ddddddd4' AND task_id = 'session:codex:dup'`); n != 1 {
		t.Fatal("archive id didn't match exactly")
	}
	if n := count(t, an, `SELECT count(DISTINCT root_id) FROM thread_roots WHERE thread_id IN ('t3:C1', 't3:C2')`); n != 1 {
		t.Fatalf("cycle split into %d tasks", n)
	}
	if n := count(t, an, `SELECT prs FROM tasks WHERE task_id = 't3:R'`); n != 1 {
		t.Fatalf("PR counted %d times", n)
	}
	if root.kind != "thread" || root.sessions != 2 || root.commits != 1 || root.add != 10 || root.prAdd != 50 || math.Abs(root.cost-8) > 1e-9 {
		t.Fatalf("root task = %+v", root)
	}
	if loose.kind != "session" || loose.commits != 1 || loose.add != 30 || math.Abs(loose.cost-4) > 1e-9 {
		t.Fatalf("loose task = %+v", loose)
	}
	if math.Abs(total-12) > 1e-9 { // root $8 + loose $4; the dup sessions cost nothing
		t.Fatalf("tasks = %v, total %v (want $12)", got, total)
	}
}
