package archive

// Commits and tasks in the analytics DB (tokenmaxx fork). A task is a T3 thread tree (a root
// thread and everything it started) or, for work outside T3, a session with no thread (with its
// subagents). Each session belongs to exactly one task, so task costs add up to total spend;
// commits join tasks through their threads or sessions.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

var commitSchemaSQL = []string{
	`CREATE TABLE IF NOT EXISTS commits (
		id           TEXT PRIMARY KEY,            -- <repo>@<sha>
		repo         TEXT NOT NULL,
		sha          TEXT NOT NULL,
		machine      TEXT NOT NULL DEFAULT '',
		authored_at  TIMESTAMPTZ,
		committed_at TIMESTAMPTZ,
		branch       TEXT NOT NULL DEFAULT '',
		subject      TEXT NOT NULL DEFAULT '',
		additions    BIGINT NOT NULL DEFAULT 0,
		deletions    BIGINT NOT NULL DEFAULT 0,
		files        BIGINT NOT NULL DEFAULT 0,
		match        TEXT NOT NULL DEFAULT '',    -- trailer (exact) | time_cwd (inferred)
		metadata     JSONB NOT NULL DEFAULT '{}',
		extracted_at TIMESTAMPTZ NOT NULL DEFAULT now()
	)`,
	`CREATE INDEX IF NOT EXISTS commits_time_idx ON commits (committed_at DESC)`,
	`CREATE TABLE IF NOT EXISTS commit_sessions (
		commit_id  TEXT NOT NULL REFERENCES commits(id) ON DELETE CASCADE,
		session_id TEXT NOT NULL,                 -- as sent: external id or <harness>:<id>
		PRIMARY KEY (commit_id, session_id)
	)`,
	`CREATE INDEX IF NOT EXISTS commit_sessions_session_idx ON commit_sessions (session_id)`,
	`CREATE TABLE IF NOT EXISTS commit_threads (
		commit_id TEXT NOT NULL REFERENCES commits(id) ON DELETE CASCADE,
		thread_id TEXT NOT NULL,                  -- <source>:<id>
		PRIMARY KEY (commit_id, thread_id)
	)`,
	`CREATE INDEX IF NOT EXISTS commit_threads_thread_idx ON commit_threads (thread_id)`,
}

// taskViewsSQL need sessions and threads only; tasksSQL (cost) needs session_usage.
var taskViewsSQL = []string{
	// The root of every thread (depth-capped, so a parent cycle can't loop).
	`CREATE VIEW thread_roots AS
	WITH RECURSIVE up AS (
		SELECT id AS thread_id, id AS root_id, parent_id, 0 AS depth FROM threads
		UNION ALL
		SELECT up.thread_id, p.id, p.parent_id, up.depth + 1
		FROM up JOIN threads p ON p.id = up.parent_id WHERE up.depth < 50
	)
	SELECT DISTINCT ON (thread_id) thread_id, root_id FROM up ORDER BY thread_id, depth DESC`,
	// The one task each session belongs to: the root thread of a thread it is in (the first, if
	// several), else its own task, a subagent going with its parent session and a proxied twin
	// with its imported transcript.
	`CREATE VIEW session_tasks AS
	SELECT s.id AS session_id,
		COALESCE(min(r.root_id), 'session:' || COALESCE(tr.parent_session_id, s.shadowed_by, s.id)) AS task_id,
		CASE WHEN min(r.root_id) IS NULL THEN 'session' ELSE 'thread' END AS task_kind
	FROM sessions s
		LEFT JOIN session_threads st ON st.session_id = s.id
		LEFT JOIN thread_roots r ON r.thread_id = st.thread_id
		LEFT JOIN session_tree tr ON tr.session_id = s.id
	GROUP BY s.id, s.shadowed_by, tr.parent_session_id`,
	// Which task(s) a commit belongs to: through the threads it names, else the sessions (by
	// archive id or external id). A commit naming two tasks counts in both.
	`CREATE VIEW commit_tasks AS
	SELECT DISTINCT c.id AS commit_id, x.task_id, x.via, c.match
	FROM commits c JOIN (
		SELECT ct.commit_id, COALESCE(r.root_id, ct.thread_id) AS task_id, 'thread'::text AS via
		FROM commit_threads ct LEFT JOIN thread_roots r ON r.thread_id = ct.thread_id
		UNION ALL
		SELECT cs.commit_id, st.task_id, 'session'
		FROM commit_sessions cs JOIN sessions s ON s.id = cs.session_id OR s.external_id = cs.session_id
			JOIN session_tasks st ON st.session_id = s.id
		WHERE NOT EXISTS (SELECT 1 FROM commit_threads ct WHERE ct.commit_id = cs.commit_id)
	) x ON x.commit_id = c.id`,
}

var tasksSQL = []string{
	// One row per task: cost (each session once, so tasks add up to total spend), what landed
	// (commits, lines) and PRs (thread tasks).
	`CREATE VIEW tasks AS
	WITH task_sessions AS (
		SELECT st.task_id, min(st.task_kind) AS kind, count(*) AS sessions, sum(u.cost_usd) AS cost_usd,
			COALESCE(sum(u.unpriced_requests), 0) AS unpriced_requests, COALESCE(sum(u.requests), 0) AS requests,
			min(s.started_at) AS started_at, max(s.ended_at) AS ended_at
		FROM session_tasks st JOIN sessions s ON s.id = st.session_id
			LEFT JOIN session_usage u ON u.session_id = st.session_id
		GROUP BY st.task_id
	), task_commits AS (
		SELECT ct.task_id, count(*) AS commits, sum(c.additions) AS additions, sum(c.deletions) AS deletions,
			sum(c.files) AS files, count(*) FILTER (WHERE ct.match = 'trailer') AS exact_commits
		FROM commit_tasks ct JOIN commits c ON c.id = ct.commit_id GROUP BY ct.task_id
	), task_prs AS (
		SELECT r.root_id AS task_id, count(*) AS prs, count(*) FILTER (WHERE p.state = 'merged') AS merged_prs,
			sum(p.additions) FILTER (WHERE p.state = 'merged') AS pr_additions,
			sum(p.deletions) FILTER (WHERE p.state = 'merged') AS pr_deletions
		FROM thread_pull_requests p JOIN thread_roots r ON r.thread_id = p.thread_id GROUP BY r.root_id
	)
	SELECT ts.task_id, ts.kind, COALESCE(t.title, s.title, '') AS title, COALESCE(t.project, s.project, '') AS project,
		COALESCE(t.created_at, ts.started_at) AS started_at, ts.ended_at, ts.sessions, ts.requests, ts.cost_usd,
		ts.unpriced_requests, COALESCE(tc.commits, 0) AS commits, COALESCE(tc.exact_commits, 0) AS exact_commits,
		COALESCE(tc.additions, 0) AS additions, COALESCE(tc.deletions, 0) AS deletions, COALESCE(tc.files, 0) AS files,
		COALESCE(tp.prs, 0) AS prs, COALESCE(tp.merged_prs, 0) AS merged_prs,
		COALESCE(tp.pr_additions, 0) AS pr_additions, COALESCE(tp.pr_deletions, 0) AS pr_deletions,
		ts.cost_usd / NULLIF(COALESCE(tc.additions, 0) + COALESCE(tc.deletions, 0), 0) AS cost_per_changed_line
	FROM task_sessions ts
		LEFT JOIN threads t ON ts.kind = 'thread' AND t.id = ts.task_id
		LEFT JOIN sessions s ON ts.kind = 'session' AND s.id = substr(ts.task_id, 9)
		LEFT JOIN task_commits tc ON tc.task_id = ts.task_id
		LEFT JOIN task_prs tp ON tp.task_id = ts.task_id`,
}

// extractCommit rewrites one commit from its archive record (or removes it when gone).
func (an *analytics) extractCommit(ctx context.Context, id string) error {
	var machine string
	var doc []byte
	err := an.a.db.QueryRowContext(ctx, `SELECT machine, doc FROM `+schema+`.commits WHERE id = $1`, id).Scan(&machine, &doc)
	if errors.Is(err, sql.ErrNoRows) {
		_, err = an.db.ExecContext(ctx, `DELETE FROM commits WHERE id = $1`, id)
		return err
	}
	if err != nil {
		return err
	}
	var c CommitIngest
	if err = json.Unmarshal(doc, &c); err != nil {
		return fmt.Errorf("commit record: %w", err)
	}
	tx, err := an.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err = tx.ExecContext(ctx, `DELETE FROM commits WHERE id = $1`, id); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO commits (id, repo, sha, machine, authored_at, committed_at, branch,
		subject, additions, deletions, files, match, metadata) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
		id, cleanText(c.Repo), c.SHA, cleanText(machine), c.AuthoredAt, firstTime(c.CommittedAt, c.AuthoredAt),
		cleanText(c.Branch), cleanText(c.Subject), c.Additions, c.Deletions, c.Files, c.Match,
		jsonOrEmpty(c.Metadata)); err != nil {
		return fmt.Errorf("commit row: %w", err)
	}
	var sess, thr [][]any
	seen := map[string]bool{}
	for _, s := range c.SessionIDs {
		if s = strings.TrimSpace(s); s != "" && !seen["s"+s] {
			seen["s"+s] = true
			sess = append(sess, []any{id, cleanText(s)})
		}
	}
	for _, t := range c.ThreadIDs {
		if t = strings.TrimSpace(t); t != "" && !seen["t"+t] {
			seen["t"+t] = true
			thr = append(thr, []any{id, cleanText(t)})
		}
	}
	if err = insertRows(ctx, tx, "commit_sessions", []string{"commit_id", "session_id"}, sess, ""); err != nil {
		return err
	}
	if err = insertRows(ctx, tx, "commit_threads", []string{"commit_id", "thread_id"}, thr, ""); err != nil {
		return err
	}
	return tx.Commit()
}

// commitGhosts lists commits in analytics that the archive no longer has.
func (an *analytics) commitGhosts(ctx context.Context) ([]string, error) {
	have := map[string]bool{}
	rows, err := an.a.db.QueryContext(ctx, `SELECT id FROM `+schema+`.commits`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			_ = rows.Close()
			return nil, err
		}
		have[id] = true
	}
	if err = closeRows(rows); err != nil {
		return nil, err
	}
	rows, err = an.db.QueryContext(ctx, `SELECT id FROM commits`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			return nil, err
		}
		if !have[id] {
			out = append(out, id)
		}
	}
	return out, rows.Err()
}
