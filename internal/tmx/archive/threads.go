package archive

// Threads (tokenmaxx fork): orchestrator threads — a T3 thread, with its provider sessions,
// parent / delegate links, pull requests and the questions and approvals it raised — imported
// next to the harness sessions they drove. Fleet transpiles each machine's orchestrator state
// into the neutral ThreadIngest shape (wiki/archive-ingest.md → Threads); the archive keeps the
// record as sent (source of truth, rebuildable analytics) and journals it for analytics, which
// flattens it into threads / thread_sessions / thread_pull_requests / thread_requests /
// thread_turns.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// ThreadIngest is one thread as an orchestrator reports it.
type ThreadIngest struct {
	ExternalID string     `json:"external_id"` // the orchestrator's thread id
	Project    string     `json:"project,omitempty"`
	CWD        string     `json:"cwd,omitempty"`
	Title      string     `json:"title,omitempty"`
	Branch     string     `json:"branch,omitempty"`
	CreatedAt  *time.Time `json:"created_at,omitempty"`
	UpdatedAt  *time.Time `json:"updated_at,omitempty"`
	ArchivedAt *time.Time `json:"archived_at,omitempty"`
	DeletedAt  *time.Time `json:"deleted_at,omitempty"`
	SettledAt  *time.Time `json:"settled_at,omitempty"`
	// ParentID is the external id of the thread that started this one (same source);
	// ParentKind says how: delegate | subagent | fork | launch.
	ParentID   string `json:"parent_id,omitempty"`
	ParentKind string `json:"parent_kind,omitempty"`
	// CreatedBy says who started the thread (user | agent | schedule …) and CreationSource where
	// (web, cli, a tool): "threads I started" needs it, a missing parent doesn't imply the user.
	CreatedBy      string `json:"created_by,omitempty"`
	CreationSource string `json:"creation_source,omitempty"`
	Provider       string `json:"provider,omitempty"`
	Model          string `json:"model,omitempty"`
	// Sessions are the harness sessions the thread ran, keyed like imported sessions
	// (harness + the session's external id).
	Sessions     []ThreadSession     `json:"sessions,omitempty"`
	PullRequests []ThreadPullRequest `json:"pull_requests,omitempty"`
	// Requests are what the thread asked the user: questions, approvals, plans to confirm.
	Requests []ThreadRequest `json:"requests,omitempty"`
	// Turns are the user-started runs (a message sent, the agent working until it stops).
	Turns    []ThreadTurn    `json:"turns,omitempty"`
	Metadata json.RawMessage `json:"metadata,omitempty"`
}

// ThreadSession links a thread to a harness session.
type ThreadSession struct {
	Harness   string `json:"harness,omitempty"` // "" = match any harness by session id
	SessionID string `json:"session_id"`
	Role      string `json:"role,omitempty"` // main | subagent | delegate
	Provider  string `json:"provider,omitempty"`
	Model     string `json:"model,omitempty"`
}

// ThreadPullRequest is a PR the thread opened or was linked to, as last seen.
type ThreadPullRequest struct {
	URL        string     `json:"url"`
	Repository string     `json:"repository,omitempty"`
	Number     int64      `json:"number,omitempty"`
	Title      string     `json:"title,omitempty"`
	State      string     `json:"state,omitempty"` // open | merged | closed | draft
	LinkedAt   *time.Time `json:"linked_at,omitempty"`
	MergedAt   *time.Time `json:"merged_at,omitempty"`
	ClosedAt   *time.Time `json:"closed_at,omitempty"`
	// Size of the change (the PR's diff).
	Additions    int64 `json:"additions,omitempty"`
	Deletions    int64 `json:"deletions,omitempty"`
	ChangedFiles int64 `json:"changed_files,omitempty"`
}

// ThreadRequest is one question / approval the thread raised.
type ThreadRequest struct {
	Kind       string     `json:"kind"`             // question | approval | plan
	Status     string     `json:"status,omitempty"` // pending | answered | approved | declined | expired
	CreatedAt  *time.Time `json:"created_at,omitempty"`
	ResolvedAt *time.Time `json:"resolved_at,omitempty"`
}

// ThreadTurn is one run of the thread.
type ThreadTurn struct {
	RequestedAt *time.Time `json:"requested_at,omitempty"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
	Status      string     `json:"status,omitempty"`
	Provider    string     `json:"provider,omitempty"`
	Model       string     `json:"model,omitempty"`
}

// ThreadBatch is the body of PUT /tmx/archive/threads.
type ThreadBatch struct {
	Source  string         `json:"source"` // the orchestrator: t3
	Machine string         `json:"machine,omitempty"`
	Threads []ThreadIngest `json:"threads"`
}

// ThreadBatchResult reports what a batch changed.
type ThreadBatchResult struct {
	Created   int `json:"created"`
	Updated   int `json:"updated"`
	Unchanged int `json:"unchanged"`
}

const maxThreadBatch = 1000

// ThreadKey is the archive id of a thread.
func ThreadKey(source, externalID string) string { return source + ":" + externalID }

// IngestThreads upserts a batch of threads. Idempotent: an unchanged thread is left alone; a
// changed one replaces its record and is re-extracted by analytics.
func (a *Archive) IngestThreads(ctx context.Context, in ThreadBatch) (*ThreadBatchResult, error) {
	in.Source = strings.TrimSpace(in.Source)
	if !validIdent(in.Source, 64) {
		return nil, fmt.Errorf("source is required (no spaces)")
	}
	if len(in.Threads) > maxThreadBatch {
		return nil, fmt.Errorf("at most %d threads per batch", maxThreadBatch)
	}
	for i := range in.Threads {
		t := &in.Threads[i]
		t.ExternalID = strings.TrimSpace(t.ExternalID)
		if !validIdent(t.ExternalID, 256) {
			return nil, fmt.Errorf("thread %d: external_id is required (no spaces)", i)
		}
		if len(t.Metadata) > 0 && !json.Valid(t.Metadata) {
			return nil, fmt.Errorf("thread %q: metadata is not valid JSON", t.ExternalID)
		}
		if strings.TrimSpace(t.ParentID) == t.ExternalID {
			return nil, fmt.Errorf("thread %q: a thread can't be its own parent", t.ExternalID)
		}
		for j, s := range t.Sessions {
			if strings.TrimSpace(s.SessionID) == "" {
				return nil, fmt.Errorf("thread %q: session %d has no session_id", t.ExternalID, j)
			}
		}
		for j, pr := range t.PullRequests {
			if strings.TrimSpace(pr.URL) == "" {
				return nil, fmt.Errorf("thread %q: pull request %d has no url", t.ExternalID, j)
			}
		}
	}
	res := &ThreadBatchResult{}
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	for _, t := range in.Threads {
		doc, errM := json.Marshal(t)
		if errM != nil {
			return nil, errM
		}
		id := ThreadKey(in.Source, t.ExternalID)
		var inserted bool
		errUp := tx.QueryRowContext(ctx, `INSERT INTO `+schema+`.threads (id, source, external_id, machine, doc,
			created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT (id) DO UPDATE SET machine = COALESCE(NULLIF(EXCLUDED.machine, ''), threads.machine),
				doc = EXCLUDED.doc, created_at = EXCLUDED.created_at, updated_at = EXCLUDED.updated_at, ingested_at = now()
			WHERE threads.doc IS DISTINCT FROM EXCLUDED.doc OR threads.machine IS DISTINCT FROM COALESCE(NULLIF(EXCLUDED.machine, ''), threads.machine)
			RETURNING (xmax = 0)`, id, in.Source, t.ExternalID, in.Machine, string(doc),
			timePtr(t.CreatedAt), timePtr(t.UpdatedAt)).Scan(&inserted)
		switch {
		case errors.Is(errUp, sql.ErrNoRows):
			res.Unchanged++
			continue
		case errUp != nil:
			return nil, fmt.Errorf("thread %q: %w", t.ExternalID, errUp)
		case inserted:
			res.Created++
		default:
			res.Updated++
		}
		if err = a.journal(ctx, tx, "thread", id); err != nil {
			return nil, fmt.Errorf("analytics journal: %w", err)
		}
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return res, nil
}

// ErrThreadNotFound is returned when a thread id is unknown.
var ErrThreadNotFound = errors.New("thread not found")

// DeleteThread removes a thread record (sessions it ran are untouched).
func (a *Archive) DeleteThread(ctx context.Context, id string) error {
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	tag, err := tx.ExecContext(ctx, `DELETE FROM `+schema+`.threads WHERE id = $1`, id)
	if err != nil {
		return err
	}
	if n, _ := tag.RowsAffected(); n == 0 {
		return ErrThreadNotFound
	}
	if err = a.journal(ctx, tx, "thread", id); err != nil {
		return fmt.Errorf("analytics journal: %w", err)
	}
	return tx.Commit()
}

// ThreadSummary is one row of the thread list.
type ThreadSummary struct {
	ID         string     `json:"id"`
	Source     string     `json:"source"`
	ExternalID string     `json:"external_id"`
	Machine    string     `json:"machine"`
	Title      string     `json:"title"`
	CreatedAt  *time.Time `json:"created_at,omitempty"`
	UpdatedAt  *time.Time `json:"updated_at,omitempty"`
	IngestedAt time.Time  `json:"ingested_at"`
	// Sessions is the thread's sessions as sent (ThreadQuery.WithSessions only).
	Sessions json.RawMessage `json:"sessions,omitempty"`
}

// ThreadQuery filters ListThreads.
type ThreadQuery struct {
	Source, Machine string
	Since           time.Time
	Limit           int
	Cursor          string // next from the previous page
	WithSessions    bool
}

// ListThreads lists threads, most recently updated first (by updated_at, then id), a page at a
// time: pass the returned next cursor back to get the following page ("" = no more).
func (a *Archive) ListThreads(ctx context.Context, q ThreadQuery) ([]ThreadSummary, string, error) {
	if q.Limit <= 0 || q.Limit > 1000 {
		q.Limit = 100
	}
	before, beforeID, err := decodeCursor(q.Cursor)
	if err != nil {
		return nil, "", err
	}
	rows, err := a.db.QueryContext(ctx, `SELECT id, source, external_id, machine, COALESCE(doc->>'title', ''),
		created_at, updated_at, ingested_at, CASE WHEN $7 THEN COALESCE(doc->'sessions', '[]') END,
		COALESCE(updated_at, 'epoch')
		FROM `+schema+`.threads
		WHERE ($1 = '' OR source = $1) AND ($2 = '' OR machine = $2) AND ingested_at >= $3
			AND ($5 = '' OR (COALESCE(updated_at, 'epoch'), id) < ($6, $5))
		ORDER BY COALESCE(updated_at, 'epoch') DESC, id DESC LIMIT $4`,
		q.Source, q.Machine, q.Since, q.Limit+1, beforeID, before, q.WithSessions)
	if err != nil {
		return nil, "", err
	}
	defer func() { _ = rows.Close() }()
	out := []ThreadSummary{}
	var lastAt time.Time
	for rows.Next() {
		var t ThreadSummary
		var created, updated sql.NullTime
		var sessions []byte
		var at time.Time
		if err = rows.Scan(&t.ID, &t.Source, &t.ExternalID, &t.Machine, &t.Title, &created, &updated, &t.IngestedAt,
			&sessions, &at); err != nil {
			return nil, "", err
		}
		if len(out) == q.Limit {
			return out, encodeCursor(lastAt, out[len(out)-1].ID), rows.Err()
		}
		t.CreatedAt, t.UpdatedAt = nullTimePtr(created), nullTimePtr(updated)
		if sessions != nil {
			t.Sessions = sessions
		}
		out = append(out, t)
		lastAt = at
	}
	return out, "", rows.Err()
}

// GetThread returns a thread record as stored.
func (a *Archive) GetThread(ctx context.Context, id string) (map[string]any, error) {
	var source, ext, machine string
	var doc []byte
	var ingested time.Time
	err := a.db.QueryRowContext(ctx, `SELECT source, external_id, machine, doc, ingested_at FROM `+schema+`.threads
		WHERE id = $1`, id).Scan(&source, &ext, &machine, &doc, &ingested)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrThreadNotFound
	}
	if err != nil {
		return nil, err
	}
	return map[string]any{"id": id, "source": source, "external_id": ext, "machine": machine,
		"ingested_at": ingested, "thread": json.RawMessage(doc)}, nil
}
