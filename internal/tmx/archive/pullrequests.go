package archive

// Pull requests (tokenmaxx fork): GitHub PRs with their reviews, review comments and outcome,
// sent by fleet. The raw GitHub JSON is kept verbatim in the blob store (content-addressed, kept
// forever, so a re-send stores nothing new); the archive indexes each PR (tmx_archive.
// pull_requests, its current state) and keeps every distinct state it was sent in as a version
// (tmx_archive.pull_request_versions, append-only). Analytics extracts the current version into
// prs / pr_reviews / pr_review_comments / pr_comments / pr_commits and the versions into
// pr_versions (analytics_prs.go).

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/tidwall/gjson"
)

// PullRequestIngest is one PR as fleet fetched it from GitHub. Pull, Reviews, ReviewComments,
// IssueComments and Commits are the GitHub REST responses verbatim (GET /repos/{o}/{r}/pulls/{n},
// …/pulls/{n}/reviews, …/pulls/{n}/comments, …/issues/{n}/comments, …/pulls/{n}/commits).
type PullRequestIngest struct {
	Repo           string          `json:"repo,omitempty"` // owner/name; default pull.base.repo.full_name
	Pull           json.RawMessage `json:"pull"`
	Reviews        json.RawMessage `json:"reviews,omitempty"`
	ReviewComments json.RawMessage `json:"review_comments,omitempty"`
	IssueComments  json.RawMessage `json:"issue_comments,omitempty"`
	Commits        json.RawMessage `json:"commits,omitempty"`
	// Outcome overrides the one derived from pull (open | merged | closed | reverted).
	Outcome string `json:"outcome,omitempty"`
	// RevertedBy names what reverted the PR: a PR ("owner/name#123") or a commit sha. Implies
	// outcome reverted.
	RevertedBy string          `json:"reverted_by,omitempty"`
	FetchedAt  *time.Time      `json:"fetched_at,omitempty"`
	Metadata   json.RawMessage `json:"metadata,omitempty"`
}

// PullRequestBatch is the body of PUT /tmx/archive/pull-requests.
type PullRequestBatch struct {
	Machine      string              `json:"machine,omitempty"`
	PullRequests []PullRequestIngest `json:"pull_requests"`
}

// PullRequestResult is the outcome of one PR: created (first version) | updated (a new version) |
// unchanged (this exact state is already a version) | error (nothing stored; Retryable = a
// server-side failure, send it again). Current is false when a newer state (by GitHub
// updated_at) was already indexed: the version is kept, the index stays.
type PullRequestResult struct {
	Index     int    `json:"index"`
	ID        string `json:"id,omitempty"`
	Status    string `json:"status"`
	Version   int    `json:"version,omitempty"`
	Current   bool   `json:"current,omitempty"`
	Error     string `json:"error,omitempty"`
	Retryable bool   `json:"retryable,omitempty"`
}

// PullRequestBatchResult counts a batch's results.
type PullRequestBatchResult struct {
	Created   int                 `json:"created"`
	Updated   int                 `json:"updated"`
	Unchanged int                 `json:"unchanged"`
	Errors    int                 `json:"errors"`
	Results   []PullRequestResult `json:"results"`
}

const maxPullRequestBatch = 200

// PullRequestKey is the archive id of a PR: lower-cased owner/name, '#', number.
func PullRequestKey(repo string, number int64) string {
	return strings.ToLower(repo) + "#" + strconv.FormatInt(number, 10)
}

var prOutcomes = map[string]bool{"": true, "open": true, "merged": true, "closed": true, "reverted": true}

// prParts are the raw documents of a PR, in blob order.
var prParts = []string{"pull", "reviews", "review_comments", "issue_comments", "commits"}

// pullFacts is what the index needs from the pull document.
type pullFacts struct {
	repo, url, title, author, state          string
	number                                   int64
	draft                                    bool
	createdAt, updatedAt, mergedAt, closedAt *time.Time
	mergeSHA, headSHA, baseSHA               string
}

func ghTime(r gjson.Result) *time.Time {
	if r.Type != gjson.String || r.String() == "" {
		return nil
	}
	t, err := time.Parse(time.RFC3339, r.String())
	if err != nil {
		return nil
	}
	t = t.UTC()
	return &t
}

// checkPullRequest validates one item and reads the facts the index needs.
func checkPullRequest(p *PullRequestIngest) (*pullFacts, error) {
	if !gjson.ValidBytes(p.Pull) || !gjson.ParseBytes(p.Pull).IsObject() {
		return nil, errors.New("pull must be the GitHub pull request object")
	}
	pull := gjson.ParseBytes(p.Pull)
	f := &pullFacts{number: pull.Get("number").Int(), repo: strings.TrimSpace(p.Repo)}
	if f.repo == "" {
		f.repo = pull.Get("base.repo.full_name").String()
	}
	if f.number <= 0 {
		return nil, errors.New("pull.number is required")
	}
	if owner, name, ok := strings.Cut(f.repo, "/"); !ok || owner == "" || name == "" || strings.Contains(name, "/") ||
		!validIdent(f.repo, 256) {
		return nil, errors.New("repo must be owner/name (or set pull.base.repo.full_name)")
	}
	for _, part := range []struct {
		name string
		raw  json.RawMessage
	}{{"reviews", p.Reviews}, {"review_comments", p.ReviewComments}, {"issue_comments", p.IssueComments}, {"commits", p.Commits}} {
		if len(part.raw) == 0 || string(part.raw) == "null" {
			continue
		}
		v := gjson.ParseBytes(part.raw)
		if !gjson.ValidBytes(part.raw) || !v.IsArray() {
			return nil, fmt.Errorf("%s must be a JSON array", part.name)
		}
		key := "id"
		if part.name == "commits" {
			key = "sha"
		}
		bad := -1
		for i, it := range v.Array() {
			if !it.Get(key).Exists() {
				bad = i
				break
			}
		}
		if bad >= 0 {
			return nil, fmt.Errorf("%s[%d] has no %s", part.name, bad, key)
		}
	}
	p.Outcome = strings.ToLower(strings.TrimSpace(p.Outcome))
	if !prOutcomes[p.Outcome] {
		return nil, errors.New("outcome must be open, merged, closed or reverted")
	}
	p.RevertedBy = strings.TrimSpace(p.RevertedBy)
	if len(p.RevertedBy) > 256 {
		return nil, errors.New("reverted_by is too long")
	}
	if !validJSONOpt(p.Metadata) {
		return nil, errors.New("metadata is not valid JSON")
	}
	f.url, f.title = pull.Get("html_url").String(), pull.Get("title").String()
	f.author, f.state, f.draft = pull.Get("user.login").String(), pull.Get("state").String(), pull.Get("draft").Bool()
	f.createdAt, f.updatedAt = ghTime(pull.Get("created_at")), ghTime(pull.Get("updated_at"))
	f.mergedAt, f.closedAt = ghTime(pull.Get("merged_at")), ghTime(pull.Get("closed_at"))
	f.mergeSHA, f.headSHA, f.baseSHA = pull.Get("merge_commit_sha").String(), pull.Get("head.sha").String(), pull.Get("base.sha").String()
	return f, nil
}

// outcome derives the PR's outcome: fleet's override, reverted when something reverted it, else
// merged / closed / open from the pull document.
func (f *pullFacts) outcome(p PullRequestIngest) string {
	switch {
	case p.Outcome != "":
		return p.Outcome
	case p.RevertedBy != "":
		return "reverted"
	case f.mergedAt != nil:
		return "merged"
	case f.state == "closed":
		return "closed"
	}
	return "open"
}

// IngestPullRequests stores a batch of PRs, one transaction each, and reports per PR. Idempotent:
// a state already stored (same documents, outcome and reverted_by) is "unchanged"; anything else
// becomes a new version — history is never overwritten.
func (a *Archive) IngestPullRequests(ctx context.Context, in PullRequestBatch) (*PullRequestBatchResult, error) {
	if len(in.PullRequests) > maxPullRequestBatch {
		return nil, fmt.Errorf("at most %d pull requests per batch", maxPullRequestBatch)
	}
	res := &PullRequestBatchResult{Results: []PullRequestResult{}}
	for i := range in.PullRequests {
		r := a.ingestPullRequest(ctx, i, in.Machine, in.PullRequests[i])
		switch r.Status {
		case "created":
			res.Created++
		case "updated":
			res.Updated++
		case "unchanged":
			res.Unchanged++
		default:
			res.Errors++
		}
		res.Results = append(res.Results, r)
	}
	return res, nil
}

func (a *Archive) ingestPullRequest(ctx context.Context, i int, machine string, p PullRequestIngest) PullRequestResult {
	out := PullRequestResult{Index: i}
	fail := func(err error, retryable bool) PullRequestResult {
		out.Status, out.Error, out.Retryable = "error", err.Error(), retryable
		return out
	}
	f, err := checkPullRequest(&p)
	if err != nil {
		return fail(err, false)
	}
	out.ID = PullRequestKey(f.repo, f.number)
	// The documents, verbatim and content-addressed (a re-send stores nothing new).
	raws := []json.RawMessage{p.Pull, p.Reviews, p.ReviewComments, p.IssueComments, p.Commits}
	hashes := make([]string, len(raws))
	for j, raw := range raws {
		if len(raw) == 0 || string(raw) == "null" {
			continue
		}
		if hashes[j], err = a.blobs.Put(ctx, raw, "application/json", "github."+prParts[j]); err != nil {
			return fail(fmt.Errorf("%s: %w", prParts[j], err), true)
		}
	}
	outcome := f.outcome(p)
	meta := jsonOrEmpty(p.Metadata)
	digest := digestOf(append(append([]string{}, hashes...), outcome, p.RevertedBy, meta)...)

	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return fail(err, true)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, "tmx_archive.pull_requests:"+out.ID); err != nil {
		return fail(err, true)
	}
	var have int
	err = tx.QueryRowContext(ctx, `SELECT version FROM `+schema+`.pull_request_versions WHERE pr_id = $1 AND digest = $2`,
		out.ID, digest).Scan(&have)
	if err == nil {
		out.Status, out.Version = "unchanged", have
		var current int
		_ = tx.QueryRowContext(ctx, `SELECT version FROM `+schema+`.pull_requests WHERE id = $1`, out.ID).Scan(&current)
		out.Current = current == have
		return out
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return fail(err, true)
	}
	if err = tx.QueryRowContext(ctx, `SELECT COALESCE(max(version), 0) + 1 FROM `+schema+`.pull_request_versions
		WHERE pr_id = $1`, out.ID).Scan(&out.Version); err != nil {
		return fail(err, true)
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO `+schema+`.pull_request_versions (pr_id, version, digest, pull_blob,
		reviews_blob, review_comments_blob, issue_comments_blob, commits_blob, state, outcome, reverted_by, merged_at,
		closed_at, head_sha, gh_updated_at, fetched_at, metadata, machine)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)`,
		out.ID, out.Version, digest, hashes[0], hashes[1], hashes[2], hashes[3], hashes[4], f.state, outcome,
		p.RevertedBy, timePtr(f.mergedAt), timePtr(f.closedAt), f.headSHA, timePtr(f.updatedAt), timePtr(p.FetchedAt),
		meta, machine); err != nil {
		return fail(err, true)
	}
	// The index follows the newest state by GitHub's updated_at (a later send of an older fetch is
	// kept as a version but doesn't move the index back).
	var inserted bool
	err = tx.QueryRowContext(ctx, `INSERT INTO `+schema+`.pull_requests (id, repo, number, url, title, author, state,
		draft, outcome, reverted_by, created_at, updated_at, merged_at, closed_at, merge_commit_sha, head_sha, base_sha,
		version, machine)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)
		ON CONFLICT (id) DO UPDATE SET repo = EXCLUDED.repo, url = EXCLUDED.url, title = EXCLUDED.title,
			author = EXCLUDED.author, state = EXCLUDED.state, draft = EXCLUDED.draft, outcome = EXCLUDED.outcome,
			reverted_by = EXCLUDED.reverted_by, created_at = EXCLUDED.created_at, updated_at = EXCLUDED.updated_at,
			merged_at = EXCLUDED.merged_at, closed_at = EXCLUDED.closed_at, merge_commit_sha = EXCLUDED.merge_commit_sha,
			head_sha = EXCLUDED.head_sha, base_sha = EXCLUDED.base_sha, version = EXCLUDED.version,
			machine = COALESCE(NULLIF(EXCLUDED.machine, ''), pull_requests.machine), ingested_at = now()
		WHERE pull_requests.updated_at IS NULL OR EXCLUDED.updated_at IS NULL OR EXCLUDED.updated_at >= pull_requests.updated_at
		RETURNING (xmax = 0)`,
		out.ID, f.repo, f.number, f.url, cleanText(f.title), f.author, f.state, f.draft, outcome, p.RevertedBy,
		timePtr(f.createdAt), timePtr(f.updatedAt), timePtr(f.mergedAt), timePtr(f.closedAt), f.mergeSHA, f.headSHA,
		f.baseSHA, out.Version, machine).Scan(&inserted)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		out.Status = "updated" // kept as a version; the index has a newer state
	case err != nil:
		return fail(err, true)
	case inserted:
		out.Status, out.Current = "created", true
	default:
		out.Status, out.Current = "updated", true
	}
	if err = a.journal(ctx, tx, "pull_request", out.ID); err != nil {
		return fail(fmt.Errorf("analytics journal: %w", err), true)
	}
	if err = tx.Commit(); err != nil {
		return fail(err, true)
	}
	return out
}

// ErrPullRequestNotFound is returned when a PR id is unknown.
var ErrPullRequestNotFound = errors.New("pull request not found")

// PullRequestQuery filters ListPullRequests.
type PullRequestQuery struct {
	Repo, Outcome, Machine string
	Since                  time.Time // ingested since
	Limit                  int
	Cursor                 string // next from the previous page
}

const prIndexCols = `jsonb_build_object('id', id, 'repo', repo, 'number', number, 'url', url, 'title', title,
	'author', author, 'state', state, 'draft', draft, 'outcome', outcome, 'reverted_by', reverted_by,
	'created_at', created_at, 'updated_at', updated_at, 'merged_at', merged_at, 'closed_at', closed_at,
	'merge_commit_sha', merge_commit_sha, 'head_sha', head_sha, 'base_sha', base_sha, 'version', version,
	'machine', machine, 'ingested_at', ingested_at)`

// ListPullRequests lists indexed PRs (current state), most recently updated on GitHub first, a
// page at a time: pass the returned next cursor back ("" = no more).
func (a *Archive) ListPullRequests(ctx context.Context, q PullRequestQuery) ([]json.RawMessage, string, error) {
	if q.Limit <= 0 || q.Limit > 1000 {
		q.Limit = 100
	}
	before, beforeID, err := decodeCursor(q.Cursor)
	if err != nil {
		return nil, "", err
	}
	rows, err := a.db.QueryContext(ctx, `SELECT `+prIndexCols+`, COALESCE(updated_at, 'epoch'), id
		FROM `+schema+`.pull_requests
		WHERE ($1 = '' OR lower(repo) = lower($1)) AND ($2 = '' OR outcome = $2) AND ($3 = '' OR machine = $3)
			AND ingested_at >= $4 AND ($5 = '' OR (COALESCE(updated_at, 'epoch'), id) < ($6, $5))
		ORDER BY COALESCE(updated_at, 'epoch') DESC, id DESC LIMIT $7`,
		q.Repo, q.Outcome, q.Machine, q.Since, beforeID, before, q.Limit+1)
	if err != nil {
		return nil, "", err
	}
	defer func() { _ = rows.Close() }()
	out := []json.RawMessage{}
	var lastAt time.Time
	var lastID string
	for rows.Next() {
		var doc []byte
		var at time.Time
		var id string
		if err = rows.Scan(&doc, &at, &id); err != nil {
			return nil, "", err
		}
		if len(out) == q.Limit {
			return out, encodeCursor(lastAt, lastID), rows.Err()
		}
		out = append(out, doc)
		lastAt, lastID = at, id
	}
	return out, "", rows.Err()
}

// GetPullRequest returns a PR's index row and every version (blob hashes of the raw documents:
// GET /tmx/archive/blobs/<hash>), oldest first.
func (a *Archive) GetPullRequest(ctx context.Context, id string) (map[string]any, error) {
	var doc []byte
	err := a.db.QueryRowContext(ctx, `SELECT `+prIndexCols+` FROM `+schema+`.pull_requests WHERE id = $1`,
		strings.ToLower(id)).Scan(&doc)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrPullRequestNotFound
	}
	if err != nil {
		return nil, err
	}
	rows, err := a.db.QueryContext(ctx, `SELECT jsonb_build_object('version', version, 'state', state, 'outcome', outcome,
		'reverted_by', reverted_by, 'merged_at', merged_at, 'closed_at', closed_at, 'head_sha', head_sha,
		'gh_updated_at', gh_updated_at, 'fetched_at', fetched_at, 'ingested_at', ingested_at, 'machine', machine,
		'metadata', metadata, 'blobs', jsonb_build_object('pull', pull_blob, 'reviews', reviews_blob,
		'review_comments', review_comments_blob, 'issue_comments', issue_comments_blob, 'commits', commits_blob))
		FROM `+schema+`.pull_request_versions WHERE pr_id = $1 ORDER BY version`, strings.ToLower(id))
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	versions := []json.RawMessage{}
	for rows.Next() {
		var v []byte
		if err = rows.Scan(&v); err != nil {
			return nil, err
		}
		versions = append(versions, v)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return map[string]any{"pull_request": json.RawMessage(doc), "versions": versions}, nil
}
