package archive

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

// ghSHA is a 40-hex sha whose leading characters differ per n.
func ghSHA(n int) string { return fmt.Sprintf("%04x", n) + strings.Repeat("0", 36) }

func ghPull(number int, title, state, mergedAt, updatedAt, body string) json.RawMessage {
	merged := "null"
	if mergedAt != "" {
		merged = `"` + mergedAt + `"`
	}
	closed := "null"
	if state == "closed" {
		closed = `"` + updatedAt + `"`
	}
	return json.RawMessage(fmt.Sprintf(`{ "number": %d, "html_url": "https://github.com/O/r/pull/%d", "title": %q,
		"body": %q, "user": {"login": "vivinmeth", "type": "User"}, "state": %q, "draft": false,
		"created_at": "2026-10-20T10:00:00Z", "updated_at": %q, "merged_at": %s, "closed_at": %s,
		"merge_commit_sha": %q, "head": {"sha": %q, "ref": "feat"}, "base": {"sha": %q, "ref": "main",
		"repo": {"full_name": "O/r"}}, "labels": [{"name": "review"}], "additions": 10, "deletions": 2, "changed_files": 1 }`,
		number, number, title, body, state, updatedAt, merged, closed, ghSHA(number), ghSHA(number+100), ghSHA(1)))
}

// PR ingest: raw documents verbatim, per-item results, idempotent re-send, state changes as new
// versions (an older fetch never moves the index back), extraction + joins to commits, sessions,
// threads, reverts and review.*.
func TestPullRequestIngest(t *testing.T) {
	a, an := openTestReview(t)
	ctx := context.Background()
	reviews := json.RawMessage(`[{"id": 501, "user": {"login": "codex-bot", "type": "Bot"}, "state": "CHANGES_REQUESTED",
		"body": "two issues", "commit_id": "abc", "submitted_at": "2026-10-20T11:00:00Z"}]`)
	comments := json.RawMessage(`[{"id": 9001, "pull_request_review_id": 501, "user": {"login": "codex-bot", "type": "Bot"},
		"path": "foo.go", "line": 12, "side": "RIGHT", "body": "nil map write", "created_at": "2026-10-20T11:00:00Z"},
		{"id": 9002, "pull_request_review_id": 501, "in_reply_to_id": 9001, "user": {"login": "vivinmeth"}, "path": "foo.go",
		"line": null, "original_line": 12, "body": "fixed"}]`)
	commits := json.RawMessage(`[{"sha": "` + ghSHA(102) + `"}]`)
	pr1 := PullRequestIngest{Pull: ghPull(1, "first", "open", "", "2026-10-20T10:00:00Z", "")}
	batch := PullRequestBatch{Machine: "alien", PullRequests: []PullRequestIngest{
		pr1,
		{Pull: json.RawMessage(`{"title": "no number"}`), Repo: "o/r"},
		{Pull: ghPull(2, "second", "closed", "2026-10-20T12:00:00Z", "2026-10-20T12:00:00Z", "fixes stuff"),
			Reviews: reviews, ReviewComments: comments, Commits: commits,
			IssueComments: json.RawMessage(`[{"id": 77, "user": {"login": "claude[bot]", "type": "Bot"}, "body": "LGTM"}]`)},
		{Pull: ghPull(3, `Revert "second"`, "closed", "2026-10-21T09:00:00Z", "2026-10-21T09:00:00Z", "Reverts O/r#2")},
		{Pull: ghPull(4, "bad reviews", "open", "", "2026-10-20T10:00:00Z", ""), Reviews: json.RawMessage(`{"id": 1}`)},
		{Pull: ghPull(5, "bad outcome", "open", "", "2026-10-20T10:00:00Z", ""), Outcome: "shipped"},
	}}
	res, err := a.IngestPullRequests(ctx, batch)
	if err != nil {
		t.Fatal(err)
	}
	if res.Created != 3 || res.Errors != 3 || res.Results[0].ID != "o/r#1" || res.Results[0].Version != 1 || !res.Results[0].Current {
		t.Fatalf("ingest = %+v", res)
	}
	for _, i := range []int{1, 4, 5} {
		if r := res.Results[i]; r.Status != "error" || r.Retryable || r.Error == "" || r.Index != i {
			t.Fatalf("item %d = %+v", i, r)
		}
	}
	// Re-send: nothing new.
	if res, err = a.IngestPullRequests(ctx, batch); err != nil || res.Unchanged != 3 || res.Created+res.Updated != 0 {
		t.Fatalf("re-send = %+v %v", res, err)
	}
	// PR 1 gets closed: a new version, the index follows.
	closedPull := ghPull(1, "first", "closed", "", "2026-10-21T10:00:00Z", "")
	res, err = a.IngestPullRequests(ctx, PullRequestBatch{PullRequests: []PullRequestIngest{{Pull: closedPull}}})
	if err != nil || res.Results[0].Status != "updated" || res.Results[0].Version != 2 || !res.Results[0].Current {
		t.Fatalf("close = %+v %v", res, err)
	}
	// The old fetch again: already a version, not current.
	res, err = a.IngestPullRequests(ctx, PullRequestBatch{PullRequests: []PullRequestIngest{pr1}})
	if err != nil || res.Results[0].Status != "unchanged" || res.Results[0].Version != 1 || res.Results[0].Current {
		t.Fatalf("old fetch = %+v %v", res, err)
	}
	// An older state not seen before: kept as version 3, the index stays on version 2.
	stale := ghPull(1, "first (renamed early)", "open", "", "2026-10-20T11:00:00Z", "")
	res, err = a.IngestPullRequests(ctx, PullRequestBatch{PullRequests: []PullRequestIngest{{Pull: stale}}})
	if err != nil || res.Results[0].Status != "updated" || res.Results[0].Version != 3 || res.Results[0].Current {
		t.Fatalf("stale = %+v %v", res, err)
	}
	got, err := a.GetPullRequest(ctx, "O/R#1")
	if err != nil {
		t.Fatal(err)
	}
	var idx struct {
		Version int    `json:"version"`
		Outcome string `json:"outcome"`
		Title   string `json:"title"`
	}
	_ = json.Unmarshal(got["pull_request"].(json.RawMessage), &idx)
	versions := got["versions"].([]json.RawMessage)
	if idx.Version != 2 || idx.Outcome != "closed" || idx.Title != "first" || len(versions) != 3 {
		t.Fatalf("pr 1 = %+v, %d versions", idx, len(versions))
	}
	// The raw document is stored verbatim.
	var v1 struct {
		Blobs map[string]string `json:"blobs"`
	}
	_ = json.Unmarshal(versions[0], &v1)
	raw, err := a.blobs.Get(ctx, v1.Blobs["pull"])
	if err != nil || string(raw) != string(pr1.Pull) {
		t.Fatalf("pull blob = %q %v", raw, err)
	}
	// Fleet reports a revert explicitly: a new version with outcome reverted.
	if res, err = a.IngestPullRequests(ctx, PullRequestBatch{PullRequests: []PullRequestIngest{{Pull: closedPull, RevertedBy: "deadbeef"}}}); err != nil ||
		res.Results[0].Version != 4 || !res.Results[0].Current {
		t.Fatalf("reverted_by = %+v %v", res, err)
	}
	list, next, err := a.ListPullRequests(ctx, PullRequestQuery{Repo: "o/R", Limit: 2})
	if err != nil || len(list) != 2 || next == "" {
		t.Fatalf("list = %d %q %v", len(list), next, err)
	}

	// Joins: a session + commit (PR 2's commit), a thread linked to PR 1, a review of PR 2.
	ts := time.Date(2026, 10, 20, 10, 0, 0, 0, time.UTC)
	if _, err = a.IngestSession(ctx, SessionIngest{Harness: "codex", ExternalID: "s-pr2", StartedAt: &ts, Messages: []MessageIngest{
		{ExternalID: "m", Seq: 1, Role: "user", TS: &ts, Message: json.RawMessage(`{"role":"user","content":"x"}`)}}}); err != nil {
		t.Fatal(err)
	}
	if _, err = a.IngestCommits(ctx, CommitBatch{Commits: []CommitIngest{
		{Repo: "github.com/O/r", SHA: ghSHA(102)[:12], SessionIDs: []string{"s-pr2"}},
		{Repo: "github.com/O/r", SHA: ghSHA(2), Subject: "Merge pull request #2"},
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err = a.IngestThreads(ctx, ThreadBatch{Source: "t3", Threads: []ThreadIngest{{ExternalID: "T1",
		PullRequests: []ThreadPullRequest{{URL: "https://github.com/O/r/pull/1", Repository: "O/r", Number: 1}}}}}); err != nil {
		t.Fatal(err)
	}
	if _, err = a.RecordReviews(ctx, []ReviewIngest{{Model: "gpt-6", Repo: "O/r", PRNumber: 2, GitHubReviewID: 501,
		Findings: []FindingIngest{{Text: "nil map write", GitHubCommentID: 9001}}}}); err != nil {
		t.Fatal(err)
	}
	drain(t, an)
	checks := []struct {
		q    string
		want int
	}{
		{`SELECT count(*) FROM prs`, 3},
		{`SELECT versions FROM prs WHERE id = 'o/r#1'`, 4},
		{`SELECT count(*) FROM pr_versions WHERE pr_id = 'o/r#1'`, 4},
		{`SELECT count(*) FROM prs WHERE id = 'o/r#1' AND outcome = 'reverted' AND reverted_by = 'deadbeef' AND version = 4`, 1},
		{`SELECT count(*) FROM pr_reviews WHERE pr_id = 'o/r#2' AND author_type = 'Bot' AND state = 'CHANGES_REQUESTED'`, 1},
		{`SELECT count(*) FROM pr_review_comments WHERE pr_id = 'o/r#2' AND review_id = 501`, 2},
		{`SELECT count(*) FROM pr_review_comments WHERE comment_id = 9002 AND line IS NULL AND original_line = 12 AND in_reply_to_id = 9001`, 1},
		{`SELECT count(*) FROM pr_comments WHERE pr_id = 'o/r#2'`, 1},
		{`SELECT count(*) FROM pr_commits WHERE pr_id = 'o/r#2'`, 3},      // commit, head, merge
		{`SELECT count(*) FROM pr_commit_links WHERE pr_id = 'o/r#2'`, 3}, // commit + head (short sha), merge
		{`SELECT count(*) FROM pr_sessions WHERE pr_id = 'o/r#2' AND session_id = 'codex:s-pr2' AND via = 'commit'`, 1},
		{`SELECT count(*) FROM pr_tasks WHERE pr_id = 'o/r#1' AND task_id = 't3:T1' AND via = 'thread'`, 1},
		{`SELECT count(*) FROM pr_tasks WHERE pr_id = 'o/r#2' AND task_id = 'session:codex:s-pr2'`, 1},
		{`SELECT count(*) FROM pr_outcomes WHERE pr_id = 'o/r#2' AND outcome = 'reverted' AND reverted_by = 'o/r#3' AND reverted_via = 'revert_pr'`, 1},
		{`SELECT count(*) FROM pr_outcomes WHERE pr_id = 'o/r#3' AND outcome = 'merged'`, 1},
		{`SELECT count(*) FROM pr_outcomes WHERE pr_id = 'o/r#1' AND reverted_via = 'fleet'`, 1},
		{`SELECT count(*) FROM prs WHERE id = 'o/r#2' AND labels = '{review}' AND data->>'title' = 'second'`, 1},
		// review.* joins PRs, their GitHub reviews and comments.
		{`SELECT count(*) FROM review.findings_judged j JOIN prs p ON lower(p.repo) = lower(j.repo) AND p.number = j.pr_number
			JOIN pr_review_comments c ON c.pr_id = p.id AND c.comment_id = j.github_comment_id
			JOIN review.reviews r ON r.id = j.review_id JOIN pr_reviews pr ON pr.pr_id = p.id AND pr.review_id = r.github_review_id`, 1},
	}
	for _, c := range checks {
		if n := count(t, an, c.q); n != c.want {
			t.Errorf("%s = %d, want %d", strings.Join(strings.Fields(c.q), " "), n, c.want)
		}
	}
	// A revert found through a commit's metadata.reverts (fleet reads "This reverts commit …").
	if _, err = a.IngestCommits(ctx, CommitBatch{Commits: []CommitIngest{{Repo: "github.com/O/r", SHA: "abcdef12",
		Metadata: json.RawMessage(`{"reverts": ["` + ghSHA(3) + `"]}`)}}}); err != nil {
		t.Fatal(err)
	}
	drain(t, an)
	if n := count(t, an, `SELECT count(*) FROM pr_outcomes WHERE pr_id = 'o/r#3' AND outcome = 'reverted' AND reverted_via = 'revert_commit'`); n != 1 {
		t.Fatal("revert commit not detected")
	}
	// Backfill re-extracts PRs from the archive (the raw documents), losing nothing.
	if _, err = an.db.Exec(`TRUNCATE prs CASCADE`); err != nil {
		t.Fatal(err)
	}
	bf, err := a.AnalyticsBackfill(ctx)
	if err != nil || bf.PRs != 3 {
		t.Fatalf("backfill = %+v %v", bf, err)
	}
	drain(t, an)
	if n := count(t, an, `SELECT count(*) FROM pr_review_comments`); n != 2 {
		t.Fatalf("after backfill: %d review comments", n)
	}
}
