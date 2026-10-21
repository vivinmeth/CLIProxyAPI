package archive

import (
	"context"
	"encoding/json"
	"math"
	"testing"
	"time"
)

// Threads land in analytics with their sessions (and those sessions' subagents), PRs and
// questions; thread_usage sums the cost of a thread and its delegates; skills are found from
// Skill tool calls and slash commands; re-sending is a no-op; deleting removes it.
func TestThreadsInAnalytics(t *testing.T) {
	a, an := openTestAnalytics(t)
	ctx := context.Background()
	ts := time.Date(2026, 10, 21, 9, 0, 0, 0, time.UTC)
	usage := `{"provider":"anthropic","model":"claude-opus-5-5","uncached_input":1000000,"output":0}` // $4 each
	ingest := func(ext string, msgs ...MessageIngest) {
		t.Helper()
		if _, err := a.IngestSession(ctx, SessionIngest{Harness: "claude-code", ExternalID: ext, StartedAt: &ts, Messages: msgs}); err != nil {
			t.Fatal(err)
		}
	}
	msg := func(id, role, body, u string) MessageIngest {
		m := MessageIngest{ExternalID: id, Seq: 1, Role: role, TS: &ts, Message: json.RawMessage(body)}
		if u != "" {
			m.Usage = json.RawMessage(u)
		}
		return m
	}
	ingest("main-1", msg("u1", "user", `{"role":"user","content":"<command-name>/code-review</command-name>"}`, ""),
		msg("a1", "assistant", `{"role":"assistant","content":[{"type":"tool_use","id":"t","name":"Skill","input":{"skill":"full-send"}}]}`, usage))
	ingest("main-1:agent-x", msg("s1", "assistant", `{"role":"assistant","content":"sub"}`, usage))
	ingest("deleg-1", msg("d1", "assistant", `{"role":"assistant","content":"review"}`, usage))

	merged := ts.Add(time.Hour)
	batch := ThreadBatch{Source: "t3", Machine: "alien", Threads: []ThreadIngest{
		{ExternalID: "T1", Title: "Ship it", Project: "tokenmaxx", CreatedAt: &ts,
			Sessions:     []ThreadSession{{Harness: "claude-code", SessionID: "main-1", Role: "main"}},
			PullRequests: []ThreadPullRequest{{URL: "https://github.com/x/y/pull/1", Number: 1, State: "MERGED", MergedAt: &merged}},
			Requests:     []ThreadRequest{{Kind: "question", Status: "answered", CreatedAt: &ts}}},
		{ExternalID: "T2", ParentID: "T1", ParentKind: "delegate", Title: "Review",
			Sessions: []ThreadSession{{SessionID: "deleg-1", Role: "delegate"}}},
	}}
	res, err := a.IngestThreads(ctx, batch)
	if err != nil || res.Created != 2 {
		t.Fatalf("ingest = %+v %v", res, err)
	}
	if res, err = a.IngestThreads(ctx, batch); err != nil || res.Unchanged != 2 {
		t.Fatalf("re-ingest = %+v %v", res, err)
	}
	drain(t, an)

	if n := count(t, an, `SELECT count(*) FROM threads WHERE id = 't3:T2' AND parent_id = 't3:T1'`); n != 1 {
		t.Fatal("delegate link missing")
	}
	if n := count(t, an, `SELECT count(*) FROM thread_pull_requests WHERE state = 'merged'`); n != 1 {
		t.Fatal("merged PR missing")
	}
	if n := count(t, an, `SELECT count(*) FROM session_threads WHERE thread_id = 't3:T1'`); n != 2 { // main + its subagent
		t.Fatalf("T1 sessions = %d", n)
	}
	var cost float64
	if err = an.db.QueryRow(`SELECT cost_usd FROM thread_usage WHERE thread_id = 't3:T1'`).Scan(&cost); err != nil || math.Abs(cost-12) > 1e-9 {
		t.Fatalf("T1 cost = %v (%v), want 12 (main + subagent + delegate)", cost, err)
	}
	skills := map[string]string{}
	rows, err := an.db.Query(`SELECT skill, via FROM skill_invocations`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var s, v string
		_ = rows.Scan(&s, &v)
		skills[s] = v
	}
	_ = rows.Close()
	if skills["full-send"] != "tool" || skills["code-review"] != "command" {
		t.Fatalf("skills = %v", skills)
	}

	if err = a.DeleteThread(ctx, "t3:T2"); err != nil {
		t.Fatal(err)
	}
	drain(t, an)
	if n := count(t, an, `SELECT count(*) FROM threads`); n != 1 {
		t.Fatalf("threads after delete = %d", n)
	}
}
