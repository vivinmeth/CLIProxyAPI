package archive

// Threads in the analytics DB (tokenmaxx fork): the archive's thread records (threads.go)
// flattened, plus views that tie threads, sessions, skills and cost together.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

var threadSchemaSQL = []string{
	`CREATE TABLE IF NOT EXISTS threads (
		id           TEXT PRIMARY KEY,          -- <source>:<external_id>
		source       TEXT NOT NULL,
		external_id  TEXT NOT NULL,
		machine      TEXT NOT NULL DEFAULT '',
		project      TEXT NOT NULL DEFAULT '',
		cwd          TEXT NOT NULL DEFAULT '',
		title        TEXT NOT NULL DEFAULT '',
		branch       TEXT NOT NULL DEFAULT '',
		created_at   TIMESTAMPTZ,
		updated_at   TIMESTAMPTZ,
		archived_at  TIMESTAMPTZ,
		deleted_at   TIMESTAMPTZ,
		settled_at   TIMESTAMPTZ,
		parent_id    TEXT,                       -- the thread that started this one (<source>:<id>)
		parent_kind  TEXT NOT NULL DEFAULT '',   -- delegate | subagent | fork | launch
		provider     TEXT NOT NULL DEFAULT '',
		model        TEXT NOT NULL DEFAULT '',
		metadata     JSONB NOT NULL DEFAULT '{}',
		extracted_at TIMESTAMPTZ NOT NULL DEFAULT now()
	)`,
	`CREATE INDEX IF NOT EXISTS threads_parent_idx ON threads (parent_id) WHERE parent_id IS NOT NULL`,
	`CREATE INDEX IF NOT EXISTS threads_created_idx ON threads (created_at DESC)`,
	`CREATE TABLE IF NOT EXISTS thread_sessions (
		thread_id  TEXT NOT NULL REFERENCES threads(id) ON DELETE CASCADE,
		harness    TEXT NOT NULL DEFAULT '',     -- '' = any harness
		session_id TEXT NOT NULL,                -- the session's external id, as imported
		role       TEXT NOT NULL DEFAULT '',     -- main | subagent | delegate
		provider   TEXT NOT NULL DEFAULT '',
		model      TEXT NOT NULL DEFAULT '',
		PRIMARY KEY (thread_id, harness, session_id)
	)`,
	`CREATE INDEX IF NOT EXISTS thread_sessions_session_idx ON thread_sessions (session_id)`,
	`CREATE TABLE IF NOT EXISTS thread_pull_requests (
		thread_id  TEXT NOT NULL REFERENCES threads(id) ON DELETE CASCADE,
		url        TEXT NOT NULL,
		repository TEXT NOT NULL DEFAULT '',
		number     BIGINT NOT NULL DEFAULT 0,
		title      TEXT NOT NULL DEFAULT '',
		state      TEXT NOT NULL DEFAULT '',     -- open | merged | closed | draft
		linked_at  TIMESTAMPTZ,
		merged_at  TIMESTAMPTZ,
		closed_at  TIMESTAMPTZ,
		PRIMARY KEY (thread_id, url)
	)`,
	`CREATE TABLE IF NOT EXISTS thread_requests (
		thread_id   TEXT NOT NULL REFERENCES threads(id) ON DELETE CASCADE,
		seq         INTEGER NOT NULL,
		kind        TEXT NOT NULL,               -- question | approval | plan
		status      TEXT NOT NULL DEFAULT '',
		created_at  TIMESTAMPTZ,
		resolved_at TIMESTAMPTZ,
		PRIMARY KEY (thread_id, seq)
	)`,
	`CREATE TABLE IF NOT EXISTS thread_turns (
		thread_id    TEXT NOT NULL REFERENCES threads(id) ON DELETE CASCADE,
		seq          INTEGER NOT NULL,
		requested_at TIMESTAMPTZ,
		completed_at TIMESTAMPTZ,
		status       TEXT NOT NULL DEFAULT '',
		provider     TEXT NOT NULL DEFAULT '',
		model        TEXT NOT NULL DEFAULT '',
		PRIMARY KEY (thread_id, seq)
	)`,
}

// threadViewsSQL don't need the requests mirror; thread_usage (in analyticsViewsSQL) does.
var threadViewsSQL = []string{
	// Subagent sessions are imported as "<parent>:<agent>": their parent session.
	`CREATE VIEW session_tree AS
	SELECT s.id AS session_id, p.id AS parent_session_id
	FROM sessions s JOIN sessions p ON p.kind = 'imported' AND p.harness = s.harness
		AND p.external_id = split_part(s.external_id, ':', 1)
	WHERE s.kind = 'imported' AND position(':' in s.external_id) > 0`,
	// Which thread each extracted session belongs to: sessions the thread lists (by harness +
	// external id, or by id alone when the harness is ''), their subagent sessions, and the
	// proxied twins of both.
	`CREATE VIEW session_threads AS
	WITH direct AS (
		SELECT s.id AS session_id, ts.thread_id, ts.role
		FROM thread_sessions ts JOIN sessions s ON s.external_id = ts.session_id
			AND (ts.harness = '' OR s.harness = ts.harness OR s.kind = 'proxy')
	), sub AS (
		SELECT st.session_id, d.thread_id, 'subagent'::text AS role
		FROM session_tree st JOIN direct d ON d.session_id = st.parent_session_id
	)
	SELECT DISTINCT ON (session_id) session_id, thread_id, role
	FROM (SELECT * FROM direct UNION ALL SELECT * FROM sub) x
	ORDER BY session_id, (role = 'subagent'), thread_id`,
	// Skills used: Skill tool calls (Claude Code, OpenCode), slash commands (Claude Code
	// <command-name>), and skill files read (Codex / OpenCode load SKILL.md). Trigger phrases in
	// prose are a search away (items, kind = 'text', role = 'user').
	`CREATE VIEW skill_invocations AS
	SELECT i.session_id, i.seq, i.idx, i.ts, 'tool'::text AS via,
		COALESCE(NULLIF(i.data->>'skill', ''), NULLIF(i.data->>'name', ''), i.name) AS skill
	FROM items i WHERE i.kind = 'tool_call' AND lower(i.name) = 'skill'
	UNION ALL
	SELECT i.session_id, i.seq, i.idx, i.ts, 'command',
		substring(i.text from '<command-name>/?([^<]+)</command-name>')
	FROM items i WHERE i.kind = 'text' AND i.role = 'user' AND i.text LIKE '%<command-name>%'
	UNION ALL
	SELECT i.session_id, i.seq, i.idx, i.ts, 'file',
		substring(COALESCE(i.data->>'filePath', i.data->>'file_path', i.data->>'path', i.data::text)
			from '/skills/([^/"]+)/SKILL\.md')
	FROM items i WHERE i.kind = 'tool_call' AND i.data::text LIKE '%/skills/%SKILL.md%'`,
}

// threadUsageSQL needs session_usage (requests mirror).
var threadUsageSQL = []string{
	// Cost of a thread and everything it started (delegates, subagent threads) through the
	// sessions they ran.
	`CREATE VIEW thread_usage AS
	WITH RECURSIVE tree AS (
		SELECT id AS root_id, id AS thread_id FROM threads
		UNION ALL
		SELECT tree.root_id, c.id FROM threads c JOIN tree ON c.parent_id = tree.thread_id
	)
	SELECT tree.root_id AS thread_id, count(DISTINCT tree.thread_id) AS threads,
		count(DISTINCT st.session_id) AS sessions, COALESCE(sum(u.requests), 0) AS requests,
		COALESCE(sum(u.prompt_tokens), 0) AS prompt_tokens, COALESCE(sum(u.output_tokens), 0) AS output_tokens,
		sum(u.cost_usd) AS cost_usd, COALESCE(sum(u.unpriced_requests), 0) AS unpriced_requests
	FROM tree LEFT JOIN session_threads st ON st.thread_id = tree.thread_id
		LEFT JOIN session_usage u ON u.session_id = st.session_id
	GROUP BY tree.root_id`,
}

// extractThread rewrites one thread from its archive record (or removes it when gone).
func (an *analytics) extractThread(ctx context.Context, id string) error {
	var source, ext, machine string
	var doc []byte
	err := an.a.db.QueryRowContext(ctx, `SELECT source, external_id, machine, doc FROM `+schema+`.threads
		WHERE id = $1`, id).Scan(&source, &ext, &machine, &doc)
	if errors.Is(err, sql.ErrNoRows) {
		_, err = an.db.ExecContext(ctx, `DELETE FROM threads WHERE id = $1`, id)
		return err
	}
	if err != nil {
		return err
	}
	var t ThreadIngest
	if err = json.Unmarshal(doc, &t); err != nil {
		return fmt.Errorf("thread record: %w", err)
	}
	var parent *string
	if p := strings.TrimSpace(t.ParentID); p != "" {
		k := ThreadKey(source, p)
		parent = &k
	}
	tx, err := an.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err = tx.ExecContext(ctx, `DELETE FROM threads WHERE id = $1`, id); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO threads (id, source, external_id, machine, project, cwd, title,
		branch, created_at, updated_at, archived_at, deleted_at, settled_at, parent_id, parent_kind, provider, model,
		metadata) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)`,
		id, source, ext, cleanText(machine), cleanText(t.Project), cleanText(t.CWD), cleanText(t.Title),
		cleanText(t.Branch), t.CreatedAt, t.UpdatedAt, t.ArchivedAt, t.DeletedAt, t.SettledAt, parent,
		cleanText(t.ParentKind), cleanText(t.Provider), cleanText(t.Model), jsonOrEmpty(t.Metadata)); err != nil {
		return fmt.Errorf("thread row: %w", err)
	}
	var sess, prs, reqs, turns [][]any
	seen := map[string]bool{}
	for _, s := range t.Sessions {
		k := strings.TrimSpace(s.Harness) + "\x00" + strings.TrimSpace(s.SessionID)
		if seen[k] {
			continue
		}
		seen[k] = true
		sess = append(sess, []any{id, cleanText(strings.TrimSpace(s.Harness)), cleanText(strings.TrimSpace(s.SessionID)),
			cleanText(s.Role), cleanText(s.Provider), cleanText(s.Model)})
	}
	seenPR := map[string]bool{}
	for _, p := range t.PullRequests {
		if seenPR[p.URL] {
			continue
		}
		seenPR[p.URL] = true
		prs = append(prs, []any{id, cleanText(p.URL), cleanText(p.Repository), p.Number, cleanText(p.Title),
			cleanText(strings.ToLower(p.State)), p.LinkedAt, p.MergedAt, p.ClosedAt})
	}
	for i, r := range t.Requests {
		reqs = append(reqs, []any{id, i, cleanText(r.Kind), cleanText(r.Status), r.CreatedAt, r.ResolvedAt})
	}
	for i, r := range t.Turns {
		turns = append(turns, []any{id, i, r.RequestedAt, r.CompletedAt, cleanText(r.Status), cleanText(r.Provider), cleanText(r.Model)})
	}
	for _, ins := range []struct {
		table string
		cols  []string
		rows  [][]any
	}{
		{"thread_sessions", []string{"thread_id", "harness", "session_id", "role", "provider", "model"}, sess},
		{"thread_pull_requests", []string{"thread_id", "url", "repository", "number", "title", "state", "linked_at", "merged_at", "closed_at"}, prs},
		{"thread_requests", []string{"thread_id", "seq", "kind", "status", "created_at", "resolved_at"}, reqs},
		{"thread_turns", []string{"thread_id", "seq", "requested_at", "completed_at", "status", "provider", "model"}, turns},
	} {
		if err = insertRows(ctx, tx, ins.table, ins.cols, ins.rows, ""); err != nil {
			return fmt.Errorf("%s: %w", ins.table, err)
		}
	}
	return tx.Commit()
}

// threadGhosts lists threads in analytics that the archive no longer has.
func (an *analytics) threadGhosts(ctx context.Context) ([]string, error) {
	have := map[string]bool{}
	rows, err := an.a.db.QueryContext(ctx, `SELECT id FROM `+schema+`.threads`)
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
	rows, err = an.db.QueryContext(ctx, `SELECT id FROM threads`)
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
