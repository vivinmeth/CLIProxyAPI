package archive

// Prompt history and session attachments in the analytics DB (tokenmaxx fork).
// prompt_history: one row per archived history line with its text (read back from the raw line
// blob), joinable to sessions by session_key = sessions.id. session_attachments: a mirror of the
// archive index (metadata only; content stays in R2).

import (
	"context"
	"fmt"
	"strings"
	"time"
)

const promptTextCap = 1 << 20 // bytes of prompt text kept per row in analytics

var promptSchemaSQL = []string{
	`CREATE TABLE IF NOT EXISTS prompt_history (
		id             TEXT PRIMARY KEY,
		machine        TEXT NOT NULL,
		harness        TEXT NOT NULL,
		ts             TIMESTAMPTZ,
		project        TEXT NOT NULL DEFAULT '',
		cwd            TEXT NOT NULL DEFAULT '',
		session_id     TEXT NOT NULL DEFAULT '',   -- the harness's session id
		session_key    TEXT NOT NULL DEFAULT '',   -- <harness>:<session_id> = sessions.id
		text           TEXT NOT NULL DEFAULT '',
		text_truncated BOOLEAN NOT NULL DEFAULT FALSE,
		text_hash      TEXT NOT NULL DEFAULT '',
		text_bytes     BIGINT NOT NULL DEFAULT 0,
		line_hash      TEXT NOT NULL,              -- archive blob of the raw line
		source_file    TEXT NOT NULL DEFAULT '',
		line_no        BIGINT NOT NULL DEFAULT 0,
		parse_warning  TEXT NOT NULL DEFAULT '',
		ingested_at    TIMESTAMPTZ,
		extracted_at   TIMESTAMPTZ NOT NULL DEFAULT now()
	)`,
	`CREATE INDEX IF NOT EXISTS prompt_history_ts_idx ON prompt_history (ts)`,
	`CREATE INDEX IF NOT EXISTS prompt_history_session_idx ON prompt_history (session_key) WHERE session_key <> ''`,
	`CREATE INDEX IF NOT EXISTS prompt_history_machine_idx ON prompt_history (harness, machine)`,
	`CREATE INDEX IF NOT EXISTS prompt_history_text_trgm_idx ON prompt_history USING gin (text gin_trgm_ops)`,
}

func (an *analytics) ensurePromptSchema(ctx context.Context) error {
	for _, stmt := range promptSchemaSQL {
		if _, err := an.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("analytics schema: %w", err)
		}
	}
	var err error
	an.attMirror, err = ensureMirror(ctx, an.a.db, an.db, schema, "session_attachments", "session_attachments", "id",
		[]string{"session_key, path", "harness, session_id", "sha256"})
	return err
}

// extractPrompts (re)writes prompt rows from the archive index and their raw lines. Returns the
// ids that failed.
func (an *analytics) extractPrompts(ctx context.Context, ids []string) map[string]error {
	failed := map[string]error{}
	failAll := func(err error) map[string]error {
		for _, id := range ids {
			failed[id] = err
		}
		return failed
	}
	rows, err := an.a.db.QueryContext(ctx, `SELECT id, machine, harness, line_hash, ts, project, cwd, session_id,
			session_key, text_hash, text_bytes, source_file, line_no, parse_warning, ingested_at
		FROM `+schema+`.prompt_history WHERE id = ANY($1::text[])`, pgTextArray(ids))
	if err != nil {
		return failAll(err)
	}
	type row struct {
		id, machine, harness, lineHash, project, cwd, session, sessionKey, textHash, file, warning string
		ts                                                                                         *time.Time
		textBytes, lineNo                                                                          int64
		ingested                                                                                   time.Time
	}
	var list []row
	for rows.Next() {
		var r row
		if err = rows.Scan(&r.id, &r.machine, &r.harness, &r.lineHash, &r.ts, &r.project, &r.cwd, &r.session,
			&r.sessionKey, &r.textHash, &r.textBytes, &r.file, &r.lineNo, &r.warning, &r.ingested); err != nil {
			_ = rows.Close()
			return failAll(err)
		}
		list = append(list, r)
	}
	if err = closeRows(rows); err != nil {
		return failAll(err)
	}
	hashes := make([]string, len(list))
	for i, r := range list {
		hashes[i] = r.lineHash
	}
	an.a.blobs.Prefetch(ctx, hashes)
	var out [][]any
	for _, r := range list {
		raw, errGet := an.a.blobs.Get(ctx, r.lineHash)
		if errGet != nil {
			failed[r.id] = fmt.Errorf("raw line %s: %w", r.lineHash, errGet)
			continue
		}
		text := parsePromptLine(raw).text
		truncated := false
		if len(text) > promptTextCap {
			text, truncated = strings.ToValidUTF8(text[:promptTextCap], ""), true
		}
		out = append(out, []any{r.id, cleanText(r.machine), cleanText(r.harness), r.ts, r.project, r.cwd, r.session,
			r.sessionKey, cleanText(text), truncated, r.textHash, r.textBytes, r.lineHash, r.file, r.lineNo, r.warning, r.ingested})
	}
	// Ids no longer in the archive (prompts are never deleted there) simply have no row.
	cols := []string{"id", "machine", "harness", "ts", "project", "cwd", "session_id", "session_key", "text",
		"text_truncated", "text_hash", "text_bytes", "line_hash", "source_file", "line_no", "parse_warning", "ingested_at"}
	set := make([]string, 0, len(cols))
	for _, c := range cols[1:] {
		set = append(set, c+" = EXCLUDED."+c)
	}
	if err = insertRows(ctx, an.db, "prompt_history", cols, out,
		` ON CONFLICT (id) DO UPDATE SET `+strings.Join(set, ", ")+`, extracted_at = now()`); err != nil {
		for _, r := range out {
			failed[r[0].(string)] = err
		}
	}
	return failed
}
