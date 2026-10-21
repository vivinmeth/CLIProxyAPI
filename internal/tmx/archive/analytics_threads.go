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
		created_by   TEXT NOT NULL DEFAULT '',   -- user | agent | schedule …
		creation_source TEXT NOT NULL DEFAULT '',
		provider     TEXT NOT NULL DEFAULT '',
		model        TEXT NOT NULL DEFAULT '',
		metadata     JSONB NOT NULL DEFAULT '{}',
		extracted_at TIMESTAMPTZ NOT NULL DEFAULT now()
	)`,
	`ALTER TABLE threads ADD COLUMN IF NOT EXISTS created_by TEXT NOT NULL DEFAULT ''`,
	`ALTER TABLE threads ADD COLUMN IF NOT EXISTS creation_source TEXT NOT NULL DEFAULT ''`,
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
	`ALTER TABLE thread_pull_requests ADD COLUMN IF NOT EXISTS additions BIGINT NOT NULL DEFAULT 0`,
	`ALTER TABLE thread_pull_requests ADD COLUMN IF NOT EXISTS deletions BIGINT NOT NULL DEFAULT 0`,
	`ALTER TABLE thread_pull_requests ADD COLUMN IF NOT EXISTS changed_files BIGINT NOT NULL DEFAULT 0`,
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
	// Every (session, thread) membership: sessions a thread lists (by harness + external id, or
	// by id alone when the harness is ''), their subagent sessions, and the proxied twins of
	// both. A session can belong to several threads (a delegate's session listed by the parent
	// too); thread_usage counts each session once per thread tree.
	`CREATE VIEW session_threads AS
	WITH direct AS (
		SELECT s.id AS session_id, ts.thread_id, ts.role
		FROM thread_sessions ts JOIN sessions s ON s.external_id = ts.session_id
			AND (ts.harness = '' OR s.harness = ts.harness OR s.kind = 'proxy')
	), sub AS (
		SELECT st.session_id, d.thread_id, 'subagent'::text AS role
		FROM session_tree st JOIN direct d ON d.session_id = st.parent_session_id
	)
	SELECT DISTINCT ON (session_id, thread_id) session_id, thread_id, role
	FROM (SELECT * FROM direct UNION ALL SELECT * FROM sub) x
	ORDER BY session_id, thread_id, (role = 'subagent')`,
	// Skills used: Skill tool calls (Claude Code, OpenCode), slash commands (Claude Code
	// <command-name>), and skill files read (Codex / OpenCode load SKILL.md: a read tool or a
	// shell command — not an edit, glob or grep; the skill is the directory holding SKILL.md, nested ones like
	// skills/.system/imagegen included). Proxied twins of imported sessions are left out (the same
	// calls would count twice). Trigger phrases in prose are a search away (items, kind = 'text',
	// role = 'user').
	`CREATE VIEW skill_invocations AS
	SELECT x.* FROM (
		SELECT i.session_id, i.seq, i.idx, i.ts, 'tool'::text AS via,
			COALESCE(NULLIF(i.data->>'skill', ''), NULLIF(i.data->>'name', ''), i.name) AS skill
		FROM items i WHERE i.kind = 'tool_call' AND lower(i.name) = 'skill'
		UNION ALL
		SELECT i.session_id, i.seq, i.idx, i.ts, 'command',
			substring(i.text from '<command-name>/?([^<]+)</command-name>')
		FROM items i WHERE i.kind = 'text' AND i.role = 'user' AND i.text LIKE '%<command-name>%'
		UNION ALL
				SELECT i.session_id, i.seq, i.idx, i.ts, 'file', m[1]
		FROM items i, regexp_matches(i.data::text, '/skills/(?:[^"/ ]+/)*?([^"/ ]+)/SKILL\.md', 'g') AS m
		WHERE i.kind = 'tool_call' AND i.data::text LIKE '%/skills/%SKILL.md%'
			AND (lower(i.name) IN ('read', 'read_file', 'view')
				-- a shell command that reads; one that writes (redirect, tee, cp, mv, sed -i, heredoc)
				-- creates or edits the skill
				OR (lower(i.name) IN ('bash', 'shell', 'exec_command', 'local_shell_call', 'run_terminal_cmd')
					AND i.data::text !~ '(>|\mtee\M|\mcp\M|\mmv\M|\minstall\M|sed -i|<<)'))
	) x JOIN sessions s ON s.id = x.session_id
	WHERE s.shadowed_by IS NULL AND x.skill ~ '^[A-Za-z0-9][A-Za-z0-9._:-]*$'`,
}

// threadUsageSQL needs session_usage (requests mirror).
var threadUsageSQL = []string{
	// Cost of a thread and everything it started (delegates, subagent threads) through the
	// sessions they ran. A row includes its children, so summing rows counts a delegate twice:
	// total spend is the sum over rows with parent_id IS NULL. own_* is the thread alone: its
	// sessions that no thread under it also lists (a session the parent lists too belongs to the
	// delegate that ran it), so own costs add up to the tree total.
	// UNION (not UNION ALL) ends the recursion even if parent links form a cycle; each session
	// counts once per tree however many of its threads list it.
	`CREATE VIEW thread_usage AS
	WITH RECURSIVE tree AS (
		SELECT id AS root_id, id AS thread_id FROM threads
		UNION
		SELECT tree.root_id, c.id FROM threads c JOIN tree ON c.parent_id = tree.thread_id
	), tree_sessions AS (
		SELECT DISTINCT tree.root_id, st.session_id
		FROM tree JOIN session_threads st ON st.thread_id = tree.thread_id
	), own AS (
		SELECT st.thread_id, count(*) AS sessions, sum(u.cost_usd) AS cost_usd
		FROM session_threads st LEFT JOIN session_usage u ON u.session_id = st.session_id
		WHERE NOT EXISTS (SELECT 1 FROM tree d JOIN session_threads sd ON sd.thread_id = d.thread_id
			WHERE d.root_id = st.thread_id AND d.thread_id <> st.thread_id AND sd.session_id = st.session_id)
		GROUP BY st.thread_id
	), totals AS (
		SELECT t.root_id AS thread_id, t.threads, count(ts.session_id) AS sessions,
			COALESCE(sum(u.requests), 0) AS requests, COALESCE(sum(u.prompt_tokens), 0) AS prompt_tokens,
			COALESCE(sum(u.output_tokens), 0) AS output_tokens, sum(u.cost_usd) AS cost_usd,
			COALESCE(sum(u.unpriced_requests), 0) AS unpriced_requests
		FROM (SELECT root_id, count(*) AS threads FROM tree GROUP BY root_id) t
			LEFT JOIN tree_sessions ts ON ts.root_id = t.root_id
			LEFT JOIN session_usage u ON u.session_id = ts.session_id
		GROUP BY t.root_id, t.threads
	)
	SELECT x.thread_id, th.parent_id, x.threads, x.sessions, x.requests, x.prompt_tokens, x.output_tokens,
		x.cost_usd, x.unpriced_requests, COALESCE(o.sessions, 0) AS own_sessions, o.cost_usd AS own_cost_usd
	FROM totals x JOIN threads th ON th.id = x.thread_id LEFT JOIN own o ON o.thread_id = x.thread_id`,
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
		branch, created_at, updated_at, archived_at, deleted_at, settled_at, parent_id, parent_kind, created_by,
		creation_source, provider, model, metadata)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)`,
		id, source, ext, cleanText(machine), cleanText(t.Project), cleanText(t.CWD), cleanText(t.Title),
		cleanText(t.Branch), t.CreatedAt, t.UpdatedAt, t.ArchivedAt, t.DeletedAt, t.SettledAt, parent,
		cleanText(t.ParentKind), cleanText(t.CreatedBy), cleanText(t.CreationSource), cleanText(t.Provider),
		cleanText(t.Model), jsonOrEmpty(t.Metadata)); err != nil {
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
			cleanText(strings.ToLower(p.State)), p.LinkedAt, p.MergedAt, p.ClosedAt, p.Additions, p.Deletions, p.ChangedFiles})
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
		{"thread_pull_requests", []string{"thread_id", "url", "repository", "number", "title", "state", "linked_at",
			"merged_at", "closed_at", "additions", "deletions", "changed_files"}, prs},
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
