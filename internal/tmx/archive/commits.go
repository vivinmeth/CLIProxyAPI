package archive

// Commits (tokenmaxx fork): git commits fleet reads from each machine's repos, with the sessions /
// threads that made them — exactly (a Session-Id: / T3-Thread: trailer) or inferred (same repo
// and time). The archive keeps each record as sent and journals it; analytics joins commits to
// tasks (a T3 thread tree, or a session with no thread) for landed-change KPIs.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// CommitIngest is one commit.
type CommitIngest struct {
	Repo        string     `json:"repo"` // remote url, or the repo path when there is none
	SHA         string     `json:"sha"`
	AuthoredAt  *time.Time `json:"authored_at,omitempty"`
	CommittedAt *time.Time `json:"committed_at,omitempty"`
	Branch      string     `json:"branch,omitempty"`
	Subject     string     `json:"subject,omitempty"`
	Additions   int64      `json:"additions"`
	Deletions   int64      `json:"deletions"`
	Files       int64      `json:"files"`
	// SessionIDs are harness session ids (external ids, as imported) or archive ids
	// ("<harness>:<id>"); ThreadIDs are thread ids ("t3:<id>").
	SessionIDs []string `json:"session_ids,omitempty"`
	ThreadIDs  []string `json:"thread_ids,omitempty"`
	// Match says how they were found: trailer (exact) | time_cwd (inferred).
	Match    string          `json:"match,omitempty"`
	Metadata json.RawMessage `json:"metadata,omitempty"`
}

// CommitBatch is the body of PUT /tmx/archive/commits.
type CommitBatch struct {
	Machine string         `json:"machine,omitempty"`
	Commits []CommitIngest `json:"commits"`
}

const maxCommitBatch = 1000

// CommitKey is the archive id of a commit.
func CommitKey(repo, sha string) string { return repo + "@" + sha }

func validSHA(s string) bool {
	if len(s) < 7 || len(s) > 64 {
		return false
	}
	for _, r := range s {
		if !(r >= '0' && r <= '9' || r >= 'a' && r <= 'f') {
			return false
		}
	}
	return true
}

// IngestCommits upserts a batch of commits. Idempotent: unchanged commits are left alone.
func (a *Archive) IngestCommits(ctx context.Context, in CommitBatch) (*ThreadBatchResult, error) {
	if len(in.Commits) > maxCommitBatch {
		return nil, fmt.Errorf("at most %d commits per batch", maxCommitBatch)
	}
	for i := range in.Commits {
		c := &in.Commits[i]
		c.Repo, c.SHA = strings.TrimSpace(c.Repo), strings.ToLower(strings.TrimSpace(c.SHA))
		if c.Repo == "" || len(c.Repo) > 1024 {
			return nil, fmt.Errorf("commit %d: repo is required", i)
		}
		if !validSHA(c.SHA) {
			return nil, fmt.Errorf("commit %d: sha must be 7-64 hex characters", i)
		}
		switch c.Match {
		case "", "trailer", "time_cwd":
		default:
			return nil, fmt.Errorf("commit %s: match must be trailer or time_cwd", c.SHA)
		}
		if c.Additions < 0 || c.Deletions < 0 || c.Files < 0 {
			return nil, fmt.Errorf("commit %s: additions, deletions and files can't be negative", c.SHA)
		}
		if len(c.Metadata) > 0 && !json.Valid(c.Metadata) {
			return nil, fmt.Errorf("commit %s: metadata is not valid JSON", c.SHA)
		}
	}
	res := &ThreadBatchResult{}
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	for _, c := range in.Commits {
		doc, errM := json.Marshal(c)
		if errM != nil {
			return nil, errM
		}
		id := CommitKey(c.Repo, c.SHA)
		var inserted bool
		errUp := tx.QueryRowContext(ctx, `INSERT INTO `+schema+`.commits (id, repo, sha, machine, doc, committed_at)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT (id) DO UPDATE SET machine = COALESCE(NULLIF(EXCLUDED.machine, ''), commits.machine),
				doc = EXCLUDED.doc, committed_at = EXCLUDED.committed_at, ingested_at = now()
			WHERE commits.doc IS DISTINCT FROM EXCLUDED.doc
				OR commits.machine IS DISTINCT FROM COALESCE(NULLIF(EXCLUDED.machine, ''), commits.machine)
			RETURNING (xmax = 0)`, id, c.Repo, c.SHA, in.Machine, string(doc), timePtr(firstTime(c.CommittedAt, c.AuthoredAt))).Scan(&inserted)
		switch {
		case errors.Is(errUp, sql.ErrNoRows):
			res.Unchanged++
			continue
		case errUp != nil:
			return nil, fmt.Errorf("commit %s: %w", c.SHA, errUp)
		case inserted:
			res.Created++
		default:
			res.Updated++
		}
		if err = a.journal(ctx, tx, "commit", id); err != nil {
			return nil, fmt.Errorf("analytics journal: %w", err)
		}
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return res, nil
}

func firstTime(ts ...*time.Time) *time.Time {
	for _, t := range ts {
		if t != nil && !t.IsZero() {
			return t
		}
	}
	return nil
}

// ErrCommitNotFound is returned when a commit id is unknown.
var ErrCommitNotFound = errors.New("commit not found")

// DeleteCommit removes a commit record.
func (a *Archive) DeleteCommit(ctx context.Context, id string) error {
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	tag, err := tx.ExecContext(ctx, `DELETE FROM `+schema+`.commits WHERE id = $1`, id)
	if err != nil {
		return err
	}
	if n, _ := tag.RowsAffected(); n == 0 {
		return ErrCommitNotFound
	}
	if err = a.journal(ctx, tx, "commit", id); err != nil {
		return fmt.Errorf("analytics journal: %w", err)
	}
	return tx.Commit()
}

// CommitQuery filters ListCommits. SessionID matches a commit that names the session by its
// external id or its archive id ("<harness>:<id>"), whichever form was sent; ThreadID is "t3:<id>".
type CommitQuery struct {
	Repo, Machine       string
	SessionID, ThreadID string
	Since               time.Time
	Limit               int
	Cursor              string // next from the previous page
}

// ListCommits lists commits, newest first (by committed_at, then id), as stored, a page at a
// time: pass the returned next cursor back to get the following page ("" = no more).
func (a *Archive) ListCommits(ctx context.Context, q CommitQuery) ([]json.RawMessage, string, error) {
	if q.Limit <= 0 || q.Limit > 1000 {
		q.Limit = 100
	}
	before, beforeID, err := decodeCursor(q.Cursor)
	if err != nil {
		return nil, "", err
	}
	// An archive id also matches its bare external id, and a bare id any "<harness>:<id>".
	sessionID := strings.TrimSpace(q.SessionID)
	_, bare, _ := strings.Cut(sessionID, ":")
	rows, err := a.db.QueryContext(ctx, `SELECT jsonb_build_object('id', id, 'machine', machine, 'ingested_at', ingested_at) || doc,
			COALESCE(committed_at, 'epoch'), id
		FROM `+schema+`.commits
		WHERE ($1 = '' OR repo = $1) AND ($2 = '' OR machine = $2) AND ingested_at >= $3
			AND ($5 = '' OR EXISTS (SELECT 1 FROM jsonb_array_elements_text(COALESCE(doc->'session_ids', '[]')) s
				WHERE s = $5 OR ($6 <> '' AND s = $6) OR right(s, length($5) + 1) = ':' || $5))
			AND ($7 = '' OR COALESCE(doc->'thread_ids', '[]') ? $7)
			AND ($8 = '' OR (COALESCE(committed_at, 'epoch'), id) < ($9, $8))
		ORDER BY COALESCE(committed_at, 'epoch') DESC, id DESC LIMIT $4`,
		q.Repo, q.Machine, q.Since, q.Limit+1, sessionID, bare, strings.TrimSpace(q.ThreadID), beforeID, before)
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
