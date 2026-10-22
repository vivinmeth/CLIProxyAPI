package archive

// Pull requests in the analytics DB (tokenmaxx fork): the archive's PR records (pullrequests.go)
// extracted from the raw GitHub documents — the PR's current state, every version, its reviews,
// review comments, conversation comments and commits — plus views joining PRs to commits,
// sessions and tasks, and an outcome that includes reverts.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/tidwall/gjson"
)

var prSchemaSQL = []string{
	`CREATE TABLE IF NOT EXISTS prs (
		id               TEXT PRIMARY KEY,        -- lower(owner/name)#number
		repo             TEXT NOT NULL,           -- owner/name
		number           BIGINT NOT NULL,
		url              TEXT NOT NULL DEFAULT '',
		title            TEXT NOT NULL DEFAULT '',
		body             TEXT NOT NULL DEFAULT '',
		author           TEXT NOT NULL DEFAULT '',
		author_association TEXT NOT NULL DEFAULT '',
		state            TEXT NOT NULL DEFAULT '', -- GitHub: open | closed
		draft            BOOLEAN NOT NULL DEFAULT FALSE,
		outcome          TEXT NOT NULL DEFAULT '', -- as sent / derived: open | merged | closed | reverted (pr_outcomes adds detected reverts)
		reverted_by      TEXT NOT NULL DEFAULT '',
		created_at       TIMESTAMPTZ,
		updated_at       TIMESTAMPTZ,
		merged_at        TIMESTAMPTZ,
		closed_at        TIMESTAMPTZ,
		merged_by        TEXT NOT NULL DEFAULT '',
		merge_commit_sha TEXT NOT NULL DEFAULT '',
		head_sha         TEXT NOT NULL DEFAULT '',
		base_sha         TEXT NOT NULL DEFAULT '',
		head_ref         TEXT NOT NULL DEFAULT '',
		base_ref         TEXT NOT NULL DEFAULT '',
		labels           TEXT[] NOT NULL DEFAULT '{}',
		additions        BIGINT NOT NULL DEFAULT 0,
		deletions        BIGINT NOT NULL DEFAULT 0,
		changed_files    BIGINT NOT NULL DEFAULT 0,
		version          INTEGER NOT NULL,        -- the archive version extracted
		versions         INTEGER NOT NULL DEFAULT 1,
		machine          TEXT NOT NULL DEFAULT '',
		data             JSONB,                   -- the GitHub pull object
		extracted_at     TIMESTAMPTZ NOT NULL DEFAULT now()
	)`,
	`CREATE INDEX IF NOT EXISTS prs_repo_idx ON prs (lower(repo), number)`,
	`CREATE INDEX IF NOT EXISTS prs_url_idx ON prs (url)`,
	`CREATE INDEX IF NOT EXISTS prs_merge_sha_idx ON prs (merge_commit_sha) WHERE merge_commit_sha <> ''`,
	`CREATE TABLE IF NOT EXISTS pr_versions (
		pr_id         TEXT NOT NULL REFERENCES prs(id) ON DELETE CASCADE,
		version       INTEGER NOT NULL,
		state         TEXT NOT NULL DEFAULT '',
		outcome       TEXT NOT NULL DEFAULT '',
		reverted_by   TEXT NOT NULL DEFAULT '',
		merged_at     TIMESTAMPTZ,
		closed_at     TIMESTAMPTZ,
		head_sha      TEXT NOT NULL DEFAULT '',
		gh_updated_at TIMESTAMPTZ,
		fetched_at    TIMESTAMPTZ,
		ingested_at   TIMESTAMPTZ,
		PRIMARY KEY (pr_id, version)
	)`,
	`CREATE TABLE IF NOT EXISTS pr_reviews (
		pr_id              TEXT NOT NULL REFERENCES prs(id) ON DELETE CASCADE,
		review_id          BIGINT NOT NULL,       -- GitHub review id
		author             TEXT NOT NULL DEFAULT '',
		author_type        TEXT NOT NULL DEFAULT '', -- User | Bot
		author_association TEXT NOT NULL DEFAULT '',
		state              TEXT NOT NULL DEFAULT '', -- APPROVED | CHANGES_REQUESTED | COMMENTED | DISMISSED | PENDING
		body               TEXT NOT NULL DEFAULT '',
		commit_id          TEXT NOT NULL DEFAULT '',
		submitted_at       TIMESTAMPTZ,
		url                TEXT NOT NULL DEFAULT '',
		data               JSONB,
		PRIMARY KEY (pr_id, review_id)
	)`,
	`CREATE TABLE IF NOT EXISTS pr_review_comments (
		pr_id              TEXT NOT NULL REFERENCES prs(id) ON DELETE CASCADE,
		comment_id         BIGINT NOT NULL,       -- GitHub comment id
		review_id          BIGINT,                -- pull_request_review_id
		in_reply_to_id     BIGINT,
		author             TEXT NOT NULL DEFAULT '',
		author_type        TEXT NOT NULL DEFAULT '',
		path               TEXT NOT NULL DEFAULT '',
		line               INTEGER,
		start_line         INTEGER,
		original_line      INTEGER,
		side               TEXT NOT NULL DEFAULT '',
		commit_id          TEXT NOT NULL DEFAULT '',
		original_commit_id TEXT NOT NULL DEFAULT '',
		diff_hunk          TEXT NOT NULL DEFAULT '',
		body               TEXT NOT NULL DEFAULT '',
		created_at         TIMESTAMPTZ,
		updated_at         TIMESTAMPTZ,
		url                TEXT NOT NULL DEFAULT '',
		data               JSONB,
		PRIMARY KEY (pr_id, comment_id)
	)`,
	`CREATE INDEX IF NOT EXISTS pr_review_comments_review_idx ON pr_review_comments (pr_id, review_id)`,
	`CREATE INDEX IF NOT EXISTS pr_review_comments_comment_idx ON pr_review_comments (comment_id)`,
	`CREATE TABLE IF NOT EXISTS pr_comments (
		pr_id       TEXT NOT NULL REFERENCES prs(id) ON DELETE CASCADE,
		comment_id  BIGINT NOT NULL,              -- GitHub issue comment id (the PR conversation)
		author      TEXT NOT NULL DEFAULT '',
		author_type TEXT NOT NULL DEFAULT '',
		body        TEXT NOT NULL DEFAULT '',
		created_at  TIMESTAMPTZ,
		updated_at  TIMESTAMPTZ,
		url         TEXT NOT NULL DEFAULT '',
		data        JSONB,
		PRIMARY KEY (pr_id, comment_id)
	)`,
	`CREATE TABLE IF NOT EXISTS pr_commits (
		pr_id TEXT NOT NULL REFERENCES prs(id) ON DELETE CASCADE,
		kind  TEXT NOT NULL,                      -- commit (in the PR) | head | merge (merged PRs)
		sha   TEXT NOT NULL,
		PRIMARY KEY (pr_id, kind, sha)
	)`,
	`CREATE INDEX IF NOT EXISTS pr_commits_sha_idx ON pr_commits (sha)`,
	`CREATE INDEX IF NOT EXISTS commits_sha_idx ON commits (sha)`,
}

// prViewsSQL need commits / tasks (taskViewsSQL) and threads.
var prViewsSQL = []string{
	// PR commits joined to ingested commits: full sha, or a commit sent with a short sha.
	`CREATE VIEW pr_commit_links AS
	SELECT pc.pr_id, pc.kind, pc.sha, c.id AS commit_id
	FROM pr_commits pc JOIN commits c ON c.sha = pc.sha
	UNION ALL
	SELECT pc.pr_id, pc.kind, pc.sha, c.id
	FROM pr_commits pc JOIN commits c ON length(c.sha) < length(pc.sha) AND c.sha = left(pc.sha, length(c.sha))`,
	// Threads a PR is linked to (T3 thread_pull_requests: by url, or repository + number).
	`CREATE VIEW pr_threads AS
	SELECT DISTINCT p.id AS pr_id, tp.thread_id
	FROM prs p JOIN thread_pull_requests tp ON tp.url = p.url
		OR (tp.number = p.number AND lower(tp.repository) = lower(p.repo))`,
	// Sessions that worked on a PR: through its threads (session_threads) or its commits
	// (commit_sessions, by archive id or a bare external id).
	`CREATE VIEW pr_sessions AS
	SELECT DISTINCT pr_id, session_id, via FROM (
		SELECT pt.pr_id, st.session_id, 'thread'::text AS via
		FROM pr_threads pt JOIN session_threads st ON st.thread_id = pt.thread_id
		UNION ALL
		SELECT l.pr_id, s.id, 'commit'
		FROM pr_commit_links l JOIN commit_sessions cs ON cs.commit_id = l.commit_id
			JOIN sessions s ON s.id = cs.session_id OR (s.kind = 'imported' AND s.external_id = cs.session_id)
	) x`,
	// Tasks a PR belongs to: its threads' root, else its commits' tasks.
	`CREATE VIEW pr_tasks AS
	SELECT DISTINCT pr_id, task_id, via FROM (
		SELECT pt.pr_id, COALESCE(r.root_id, pt.thread_id) AS task_id, 'thread'::text AS via
		FROM pr_threads pt LEFT JOIN thread_roots r ON r.thread_id = pt.thread_id
		UNION ALL
		SELECT l.pr_id, ct.task_id, 'commit'
		FROM pr_commit_links l JOIN commit_tasks ct ON ct.commit_id = l.commit_id
	) x`,
	// The outcome with reverts detected: fleet's reverted_by, a merged GitHub revert PR ("Reverts
	// owner/name#n" in its body), or an ingested commit whose metadata.reverts lists the PR's merge
	// commit (fleet reads "This reverts commit <sha>." from commit messages).
	`CREATE VIEW pr_outcomes AS
	SELECT p.id AS pr_id, p.repo, p.number,
		CASE WHEN p.reverted_by <> '' OR rp.id IS NOT NULL OR rc.id IS NOT NULL THEN 'reverted' ELSE p.outcome END AS outcome,
		COALESCE(NULLIF(p.reverted_by, ''), rp.id, rc.sha) AS reverted_by,
		CASE WHEN p.reverted_by <> '' THEN 'fleet' WHEN rp.id IS NOT NULL THEN 'revert_pr'
			WHEN rc.id IS NOT NULL THEN 'revert_commit' END AS reverted_via,
		COALESCE(rp.merged_at, rc.committed_at) AS reverted_at
	FROM prs p
	LEFT JOIN LATERAL (
		SELECT r.id, r.merged_at FROM prs r
		WHERE p.merged_at IS NOT NULL AND r.id <> p.id AND r.merged_at IS NOT NULL AND lower(r.repo) = lower(p.repo)
			AND r.body ~* ('(^|[^A-Za-z0-9_./-])reverts[[:space:]]+' || regexp_replace(p.repo, '([^A-Za-z0-9_/])', '\\\1', 'g')
				|| '#' || p.number || '([^0-9]|$)')
		ORDER BY r.merged_at LIMIT 1
	) rp ON true
	LEFT JOIN LATERAL (
		SELECT c.id, c.sha, c.committed_at FROM commits c
		WHERE p.merge_commit_sha <> '' AND p.merged_at IS NOT NULL
			AND COALESCE(c.metadata->'reverts', '[]') ? p.merge_commit_sha
		ORDER BY c.committed_at NULLS LAST LIMIT 1
	) rc ON true`,
}

// extractPullRequest rewrites one PR from its archive record (or removes it when gone).
func (an *analytics) extractPullRequest(ctx context.Context, id string) error {
	var repo, machine string
	var number int64
	var version int
	err := an.a.db.QueryRowContext(ctx, `SELECT repo, number, version, machine FROM `+schema+`.pull_requests WHERE id = $1`,
		id).Scan(&repo, &number, &version, &machine)
	if errors.Is(err, sql.ErrNoRows) {
		_, err = an.db.ExecContext(ctx, `DELETE FROM prs WHERE id = $1`, id)
		return err
	}
	if err != nil {
		return err
	}
	type ver struct {
		version                              int
		state, outcome, revertedBy, headSHA  string
		mergedAt, closedAt, ghUpdated, fetch sql.NullTime
		ingested                             time.Time
		blobs                                [5]string
	}
	rows, err := an.a.db.QueryContext(ctx, `SELECT version, state, outcome, reverted_by, head_sha, merged_at, closed_at,
		gh_updated_at, fetched_at, ingested_at, pull_blob, reviews_blob, review_comments_blob, issue_comments_blob, commits_blob
		FROM `+schema+`.pull_request_versions WHERE pr_id = $1 ORDER BY version`, id)
	if err != nil {
		return err
	}
	var vers []ver
	var cur *ver
	for rows.Next() {
		var v ver
		if err = rows.Scan(&v.version, &v.state, &v.outcome, &v.revertedBy, &v.headSHA, &v.mergedAt, &v.closedAt,
			&v.ghUpdated, &v.fetch, &v.ingested, &v.blobs[0], &v.blobs[1], &v.blobs[2], &v.blobs[3], &v.blobs[4]); err != nil {
			_ = rows.Close()
			return err
		}
		vers = append(vers, v)
	}
	if err = closeRows(rows); err != nil {
		return err
	}
	for i := range vers {
		if vers[i].version == version {
			cur = &vers[i]
		}
	}
	if cur == nil {
		return fmt.Errorf("pull request %s: version %d not found", id, version)
	}
	var docs [5]gjson.Result
	var hashes []string
	for _, h := range cur.blobs {
		if h != "" {
			hashes = append(hashes, h)
		}
	}
	an.a.blobs.Prefetch(ctx, hashes)
	for i, h := range cur.blobs {
		if h == "" {
			continue
		}
		raw, errB := an.a.blobs.Get(ctx, h)
		if errB != nil {
			return fmt.Errorf("%s blob %s: %w", prParts[i], h, errB)
		}
		docs[i] = gjson.ParseBytes(raw)
	}
	pull := docs[0]
	tx, err := an.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err = tx.ExecContext(ctx, `DELETE FROM prs WHERE id = $1`, id); err != nil {
		return err
	}
	var labels []string
	for _, l := range pull.Get("labels.#.name").Array() {
		labels = append(labels, cleanText(l.String()))
	}
	if labels == nil {
		labels = []string{}
	}
	str := func(r gjson.Result, path string) string { return cleanText(r.Get(path).String()) }
	if _, err = tx.ExecContext(ctx, `INSERT INTO prs (id, repo, number, url, title, body, author, author_association,
		state, draft, outcome, reverted_by, created_at, updated_at, merged_at, closed_at, merged_by, merge_commit_sha,
		head_sha, base_sha, head_ref, base_ref, labels, additions, deletions, changed_files, version, versions, machine, data)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23::text[],$24,$25,$26,$27,$28,$29,$30)`,
		id, repo, number, str(pull, "html_url"), str(pull, "title"), str(pull, "body"), str(pull, "user.login"),
		str(pull, "author_association"), cur.state, pull.Get("draft").Bool(), cur.outcome, cur.revertedBy,
		timePtr(ghTime(pull.Get("created_at"))), timePtr(ghTime(pull.Get("updated_at"))), nullTimePtr(cur.mergedAt),
		nullTimePtr(cur.closedAt), str(pull, "merged_by.login"), str(pull, "merge_commit_sha"), cur.headSHA,
		str(pull, "base.sha"), str(pull, "head.ref"), str(pull, "base.ref"), pgTextArray(labels),
		pull.Get("additions").Int(), pull.Get("deletions").Int(), pull.Get("changed_files").Int(), version, len(vers),
		cleanText(machine), nullableJSON([]byte(pull.Raw))); err != nil {
		return fmt.Errorf("pr row: %w", err)
	}
	var vrows, rrows, crows, irows, krows [][]any
	for _, v := range vers {
		vrows = append(vrows, []any{id, v.version, v.state, v.outcome, v.revertedBy, nullTimePtr(v.mergedAt),
			nullTimePtr(v.closedAt), v.headSHA, nullTimePtr(v.ghUpdated), nullTimePtr(v.fetch), v.ingested})
	}
	nullID := func(r gjson.Result) any {
		if r.Exists() && r.Type == gjson.Number {
			return r.Int()
		}
		return nil
	}
	nullInt := nullID
	seen := map[string]bool{}
	for _, r := range docs[1].Array() {
		key := "r" + r.Get("id").Raw
		if seen[key] {
			continue
		}
		seen[key] = true
		rrows = append(rrows, []any{id, r.Get("id").Int(), str(r, "user.login"), str(r, "user.type"),
			str(r, "author_association"), str(r, "state"), str(r, "body"), str(r, "commit_id"),
			timePtr(ghTime(r.Get("submitted_at"))), str(r, "html_url"), nullableJSON([]byte(r.Raw))})
	}
	for _, c := range docs[2].Array() {
		key := "c" + c.Get("id").Raw
		if seen[key] {
			continue
		}
		seen[key] = true
		crows = append(crows, []any{id, c.Get("id").Int(), nullID(c.Get("pull_request_review_id")),
			nullID(c.Get("in_reply_to_id")), str(c, "user.login"), str(c, "user.type"), str(c, "path"),
			nullInt(c.Get("line")), nullInt(c.Get("start_line")), nullInt(c.Get("original_line")), str(c, "side"),
			str(c, "commit_id"), str(c, "original_commit_id"), str(c, "diff_hunk"), str(c, "body"),
			timePtr(ghTime(c.Get("created_at"))), timePtr(ghTime(c.Get("updated_at"))), str(c, "html_url"),
			nullableJSON([]byte(c.Raw))})
	}
	for _, c := range docs[3].Array() {
		key := "i" + c.Get("id").Raw
		if seen[key] {
			continue
		}
		seen[key] = true
		irows = append(irows, []any{id, c.Get("id").Int(), str(c, "user.login"), str(c, "user.type"), str(c, "body"),
			timePtr(ghTime(c.Get("created_at"))), timePtr(ghTime(c.Get("updated_at"))), str(c, "html_url"),
			nullableJSON([]byte(c.Raw))})
	}
	addCommit := func(kind, sha string) {
		sha = strings.ToLower(strings.TrimSpace(sha))
		if sha == "" || seen[kind+sha] {
			return
		}
		seen[kind+sha] = true
		krows = append(krows, []any{id, kind, sha})
	}
	for _, c := range docs[4].Array() {
		addCommit("commit", c.Get("sha").String())
	}
	addCommit("head", cur.headSHA)
	if cur.mergedAt.Valid {
		addCommit("merge", pull.Get("merge_commit_sha").String())
	}
	for _, t := range []struct {
		table string
		cols  []string
		rows  [][]any
	}{
		{"pr_versions", []string{"pr_id", "version", "state", "outcome", "reverted_by", "merged_at", "closed_at", "head_sha",
			"gh_updated_at", "fetched_at", "ingested_at"}, vrows},
		{"pr_reviews", []string{"pr_id", "review_id", "author", "author_type", "author_association", "state", "body",
			"commit_id", "submitted_at", "url", "data"}, rrows},
		{"pr_review_comments", []string{"pr_id", "comment_id", "review_id", "in_reply_to_id", "author", "author_type", "path",
			"line", "start_line", "original_line", "side", "commit_id", "original_commit_id", "diff_hunk", "body",
			"created_at", "updated_at", "url", "data"}, crows},
		{"pr_comments", []string{"pr_id", "comment_id", "author", "author_type", "body", "created_at", "updated_at", "url",
			"data"}, irows},
		{"pr_commits", []string{"pr_id", "kind", "sha"}, krows},
	} {
		if err = insertRows(ctx, tx, t.table, t.cols, t.rows, ""); err != nil {
			return fmt.Errorf("%s: %w", t.table, err)
		}
	}
	return tx.Commit()
}
