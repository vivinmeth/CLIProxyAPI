package archive

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/requestlog"
)

// SessionIngest is the body of PUT /v8/management/tmx/archive/sessions. See
// wiki/archive-ingest.md for the contract fleet transpilers target.
type SessionIngest struct {
	Harness        string          `json:"harness"`
	ExternalID     string          `json:"external_id"`
	HarnessVersion string          `json:"harness_version,omitempty"`
	Machine        string          `json:"machine,omitempty"`
	Project        string          `json:"project,omitempty"`
	CWD            string          `json:"cwd,omitempty"`
	Title          string          `json:"title,omitempty"`
	ViaProxy       *bool           `json:"via_proxy,omitempty"`
	StartedAt      *time.Time      `json:"started_at,omitempty"`
	EndedAt        *time.Time      `json:"ended_at,omitempty"`
	Metadata       json.RawMessage `json:"metadata,omitempty"`
	Messages       []MessageIngest `json:"messages"`
	Sources        []SourceIngest  `json:"sources,omitempty"`
	// Replace makes the body the complete session: stored messages and imported usage rows that
	// are not in it are removed. Raw sources are never removed (the original always survives).
	Replace bool `json:"replace,omitempty"`
}

// MessageIngest is one message/event of an ingested session.
type MessageIngest struct {
	ExternalID string          `json:"external_id"`
	Seq        int64           `json:"seq"`
	ParentID   string          `json:"parent_id,omitempty"`
	TS         *time.Time      `json:"ts,omitempty"`
	Role       string          `json:"role"`
	Model      string          `json:"model,omitempty"`
	Message    json.RawMessage `json:"message"`
	Usage      json.RawMessage `json:"usage,omitempty"`
	Metadata   json.RawMessage `json:"metadata,omitempty"`
}

// SourceIngest references a raw source file already uploaded with PUT .../blobs/{sha256}.
type SourceIngest struct {
	Name string `json:"name"`
	Blob string `json:"blob"`
}

// IngestResult reports what an ingest call changed.
type IngestResult struct {
	SessionID    string   `json:"session_id"`
	Created      bool     `json:"created"`
	Messages     int      `json:"messages"`
	Inserted     int      `json:"inserted"`
	Updated      int      `json:"updated"`
	Unchanged    int      `json:"unchanged"`
	Sources      int      `json:"sources"`
	MissingBlobs []string `json:"missing_blobs,omitempty"`
	ViaProxy     *bool    `json:"via_proxy,omitempty"`
	UsageRows    int      `json:"usage_rows"`
	Removed      int      `json:"removed,omitempty"`       // messages dropped by replace
	UsageRemoved int64    `json:"usage_removed,omitempty"` // usage rows dropped by replace
}

// SessionKey is the archive id of an ingested session.
func SessionKey(harness, externalID string) string { return harness + ":" + externalID }

func validIdent(s string, max int) bool {
	if s == "" || len(s) > max {
		return false
	}
	for _, r := range s {
		if r < 0x21 || r == 0x7f {
			return false
		}
	}
	return true
}

func rawOrEmpty(r json.RawMessage, empty string) string {
	if len(strings.TrimSpace(string(r))) == 0 {
		return empty
	}
	return string(r)
}

// IngestSession upserts a session. Idempotent: re-sending the same messages changes nothing;
// sending more messages (a weekly re-run) adds only the new ones; a changed message (improved
// transpiler) replaces the stored version.
func (a *Archive) IngestSession(ctx context.Context, in SessionIngest) (*IngestResult, error) {
	in.Harness = strings.TrimSpace(in.Harness)
	in.ExternalID = strings.TrimSpace(in.ExternalID)
	if !validIdent(in.Harness, 64) || !validIdent(in.ExternalID, 256) {
		return nil, fmt.Errorf("harness and external_id are required (no spaces)")
	}
	if len(in.Metadata) > 0 && !json.Valid(in.Metadata) {
		return nil, fmt.Errorf("metadata is not valid JSON")
	}
	id := SessionKey(in.Harness, in.ExternalID)
	res := &IngestResult{SessionID: id, Messages: len(in.Messages)}

	// Raw sources must already be uploaded, so the original survives any transpiler bug.
	for _, src := range in.Sources {
		if !ValidHash(src.Blob) || strings.TrimSpace(src.Name) == "" {
			return nil, fmt.Errorf("source %q: name and sha256 blob required", src.Name)
		}
	}
	hashes := make([]string, 0, len(in.Sources))
	for _, src := range in.Sources {
		hashes = append(hashes, src.Blob)
	}
	missing, err := a.blobs.Missing(ctx, hashes)
	if err != nil {
		return nil, err
	}
	if len(missing) > 0 {
		res.MissingBlobs = missing
		return res, fmt.Errorf("upload missing source blobs first")
	}

	// Store message bodies (media extracted, deduplicated) before touching the index.
	type prepared struct {
		m    MessageIngest
		hash string
	}
	prep := make([]prepared, 0, len(in.Messages))
	seen := map[string]bool{}
	for i, m := range in.Messages {
		m.ExternalID = strings.TrimSpace(m.ExternalID)
		if m.ExternalID == "" {
			m.ExternalID = fmt.Sprintf("seq-%d", m.Seq)
		}
		if seen[m.ExternalID] {
			return nil, fmt.Errorf("message %d: duplicate external_id %q", i, m.ExternalID)
		}
		seen[m.ExternalID] = true
		if !json.Valid(m.Message) {
			return nil, fmt.Errorf("message %q: message is not valid JSON", m.ExternalID)
		}
		if (len(m.Usage) > 0 && !json.Valid(m.Usage)) || (len(m.Metadata) > 0 && !json.Valid(m.Metadata)) {
			return nil, fmt.Errorf("message %q: usage/metadata is not valid JSON", m.ExternalID)
		}
		wrapped, _ := json.Marshal(map[string]json.RawMessage{"message": m.Message})
		hash, errSplit := a.splitBody(ctx, wrapped, "import.message")
		if errSplit != nil {
			return nil, errSplit
		}
		prep = append(prep, prepared{m: m, hash: hash})
	}

	viaProxy := in.ViaProxy
	if viaProxy == nil {
		var linked bool
		if errLink := a.db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM `+schema+`.exchanges
			WHERE client_session_id = $1 OR session_id = $1)`, in.ExternalID).Scan(&linked); errLink == nil && linked {
			viaProxy = &linked
		}
	}
	res.ViaProxy = viaProxy

	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	var created bool
	err = tx.QueryRowContext(ctx, `INSERT INTO `+schema+`.sessions (id, harness, external_id,
		harness_version, machine, project, cwd, title, via_proxy, started_at, ended_at, metadata)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
		ON CONFLICT (id) DO UPDATE SET
			harness_version = COALESCE(NULLIF(EXCLUDED.harness_version, ''), sessions.harness_version),
			machine = COALESCE(NULLIF(EXCLUDED.machine, ''), sessions.machine),
			project = COALESCE(NULLIF(EXCLUDED.project, ''), sessions.project),
			cwd = COALESCE(NULLIF(EXCLUDED.cwd, ''), sessions.cwd),
			title = COALESCE(NULLIF(EXCLUDED.title, ''), sessions.title),
			via_proxy = COALESCE(EXCLUDED.via_proxy, sessions.via_proxy),
			started_at = LEAST(sessions.started_at, EXCLUDED.started_at),
			ended_at = GREATEST(sessions.ended_at, EXCLUDED.ended_at),
			metadata = sessions.metadata || EXCLUDED.metadata,
			updated_at = now()
		RETURNING (xmax = 0)`,
		id, in.Harness, in.ExternalID, in.HarnessVersion, in.Machine, in.Project, in.CWD, in.Title,
		viaProxy, timePtr(in.StartedAt), timePtr(in.EndedAt), rawOrEmpty(in.Metadata, "{}")).Scan(&created)
	if err != nil {
		return nil, fmt.Errorf("session upsert: %w", err)
	}
	res.Created = created

	for _, p := range prep {
		var old string
		errOld := tx.QueryRowContext(ctx, `SELECT message FROM `+schema+`.session_messages
			WHERE session_id = $1 AND external_id = $2`, id, p.m.ExternalID).Scan(&old)
		switch {
		case errors.Is(errOld, sql.ErrNoRows):
			res.Inserted++
		case errOld != nil:
			return nil, errOld
		case old == p.hash:
			res.Unchanged++
		default:
			res.Updated++
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO `+schema+`.session_messages (session_id,
			external_id, seq, parent_id, ts, role, model, message, usage, metadata)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
			ON CONFLICT (session_id, external_id) DO UPDATE SET seq = EXCLUDED.seq,
				parent_id = EXCLUDED.parent_id, ts = EXCLUDED.ts, role = EXCLUDED.role,
				model = EXCLUDED.model, message = EXCLUDED.message, usage = EXCLUDED.usage,
				metadata = EXCLUDED.metadata, ingested_at = now()
			WHERE session_messages.message IS DISTINCT FROM EXCLUDED.message
				OR session_messages.seq IS DISTINCT FROM EXCLUDED.seq
				OR session_messages.usage IS DISTINCT FROM EXCLUDED.usage
				OR session_messages.metadata IS DISTINCT FROM EXCLUDED.metadata`,
			id, p.m.ExternalID, p.m.Seq, p.m.ParentID, timePtr(p.m.TS), p.m.Role, p.m.Model, p.hash,
			nullJSON(p.m.Usage), rawOrEmpty(p.m.Metadata, "{}")); err != nil {
			return nil, fmt.Errorf("message %q: %w", p.m.ExternalID, err)
		}
	}
	if in.Replace {
		keep := make([]string, 0, len(prep))
		for _, p := range prep {
			keep = append(keep, p.m.ExternalID)
		}
		tag, errDel := tx.ExecContext(ctx, `DELETE FROM `+schema+`.session_messages WHERE session_id = $1
			AND NOT (external_id = ANY($2::text[]))`, id, pgTextArray(keep))
		if errDel != nil {
			return nil, fmt.Errorf("replace: %w", errDel)
		}
		n, _ := tag.RowsAffected()
		res.Removed = int(n)
	}
	for _, src := range in.Sources {
		var size int64
		_ = tx.QueryRowContext(ctx, `SELECT size FROM `+schema+`.blobs WHERE hash = $1`, src.Blob).Scan(&size)
		tag, errSrc := tx.ExecContext(ctx, `INSERT INTO `+schema+`.session_sources (session_id, name, blob, size)
			VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING`, id, src.Name, src.Blob, size)
		if errSrc != nil {
			return nil, errSrc
		}
		if n, _ := tag.RowsAffected(); n > 0 {
			res.Sources++
		}
	}
	if err = a.journal(ctx, tx, "session", id); err != nil {
		return nil, fmt.Errorf("analytics journal: %w", err)
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	// Usage rows go to tmx.requests (source extern:<harness>) so cost views can include them.
	if res.UsageRows, err = a.writeUsage(ctx, in, id, viaProxy); err != nil {
		return res, fmt.Errorf("usage rows: %w", err)
	}
	if in.Replace {
		keep := []string{}
		for _, row := range usageRows(in, id, viaProxy) {
			keep = append(keep, row.ImportRef)
		}
		if res.UsageRemoved, err = requestlog.Default().DeleteImported(ctx, id+"#", keep); err != nil {
			return res, fmt.Errorf("replace usage rows: %w", err)
		}
	}
	// Usage rows are written after the commit; journal again so analytics copies them.
	_ = a.journal(ctx, a.db, "session", id)
	return res, nil
}

// DeleteResult reports what DeleteSession removed.
type DeleteResult struct {
	SessionID string `json:"session_id"`
	Messages  int64  `json:"messages"`
	Sources   int64  `json:"sources"`
	UsageRows int64  `json:"usage_rows"`
}

// DeleteSession removes an imported session, its messages, source links and imported usage rows.
// Content blobs stay (they are shared and content-addressed); proxied traffic is untouched.
func (a *Archive) DeleteSession(ctx context.Context, id string) (*DeleteResult, error) {
	res := &DeleteResult{SessionID: id}
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	if err = tx.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM `+schema+`.session_messages WHERE session_id = $1),
		(SELECT count(*) FROM `+schema+`.session_sources WHERE session_id = $1)`, id).Scan(&res.Messages, &res.Sources); err != nil {
		return nil, err
	}
	tag, err := tx.ExecContext(ctx, `DELETE FROM `+schema+`.sessions WHERE id = $1`, id)
	if err != nil {
		return nil, err
	}
	if n, _ := tag.RowsAffected(); n == 0 {
		return nil, ErrSessionNotFound
	}
	if err = a.journal(ctx, tx, "session", id); err != nil {
		return nil, fmt.Errorf("analytics journal: %w", err)
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	if res.UsageRows, err = requestlog.Default().DeleteImported(ctx, id+"#", nil); err != nil {
		return res, fmt.Errorf("usage rows: %w", err)
	}
	return res, nil
}

// ErrSessionNotFound is returned when a session id is unknown.
var ErrSessionNotFound = errors.New("session not found")

// pgTextArray renders a Postgres text[] literal with every element quoted.
func pgTextArray(items []string) string {
	var b strings.Builder
	b.WriteByte('{')
	for i, it := range items {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteByte('"')
		b.WriteString(strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(it))
		b.WriteByte('"')
	}
	b.WriteByte('}')
	return b.String()
}

func timePtr(t *time.Time) any {
	if t == nil || t.IsZero() {
		return nil
	}
	return t.UTC()
}

func nullJSON(r json.RawMessage) any {
	if len(strings.TrimSpace(string(r))) == 0 {
		return nil
	}
	return string(r)
}

// SessionSummary is a row of the session list.
type SessionSummary struct {
	ID             string          `json:"id"`
	Harness        string          `json:"harness"`
	ExternalID     string          `json:"external_id"`
	HarnessVersion string          `json:"harness_version"`
	Machine        string          `json:"machine"`
	Project        string          `json:"project"`
	CWD            string          `json:"cwd"`
	Title          string          `json:"title"`
	ViaProxy       *bool           `json:"via_proxy"`
	StartedAt      *time.Time      `json:"started_at"`
	EndedAt        *time.Time      `json:"ended_at"`
	Metadata       json.RawMessage `json:"metadata"`
	Messages       int64           `json:"messages"`
	UpdatedAt      time.Time       `json:"updated_at"`
}

// ListSessions lists ingested sessions, most recently updated first.
func (a *Archive) ListSessions(ctx context.Context, harness, machine string, since time.Time, limit int) ([]SessionSummary, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := a.db.QueryContext(ctx, `SELECT s.id, s.harness, s.external_id, s.harness_version,
		s.machine, s.project, s.cwd, s.title, s.via_proxy, s.started_at, s.ended_at, s.metadata,
		(SELECT count(*) FROM `+schema+`.session_messages m WHERE m.session_id = s.id), s.updated_at
		FROM `+schema+`.sessions s
		WHERE ($1 = '' OR s.harness = $1) AND ($2 = '' OR s.machine = $2) AND s.updated_at >= $3
		ORDER BY s.updated_at DESC LIMIT $4`, harness, machine, since, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := []SessionSummary{}
	for rows.Next() {
		var s SessionSummary
		var meta []byte
		if err = rows.Scan(&s.ID, &s.Harness, &s.ExternalID, &s.HarnessVersion, &s.Machine, &s.Project,
			&s.CWD, &s.Title, &s.ViaProxy, &s.StartedAt, &s.EndedAt, &meta, &s.Messages, &s.UpdatedAt); err != nil {
			return nil, err
		}
		s.Metadata = meta
		out = append(out, s)
	}
	return out, rows.Err()
}

// StoredMessage is a message as returned by GetSession.
type StoredMessage struct {
	ExternalID string          `json:"external_id"`
	Seq        int64           `json:"seq"`
	ParentID   string          `json:"parent_id"`
	TS         *time.Time      `json:"ts"`
	Role       string          `json:"role"`
	Model      string          `json:"model"`
	Message    json.RawMessage `json:"message"`
	Usage      json.RawMessage `json:"usage,omitempty"`
	Metadata   json.RawMessage `json:"metadata"`
}

// GetSession returns a session with its messages rebuilt (media re-inlined) and its sources.
func (a *Archive) GetSession(ctx context.Context, id string) (map[string]any, error) {
	list, err := a.db.QueryContext(ctx, `SELECT external_id, seq, parent_id, ts, role, model, message,
		usage, metadata FROM `+schema+`.session_messages WHERE session_id = $1 ORDER BY seq, ts`, id)
	if err != nil {
		return nil, err
	}
	defer func() { _ = list.Close() }()
	msgs := []StoredMessage{}
	for list.Next() {
		var m StoredMessage
		var hash string
		var usage, meta []byte
		if err = list.Scan(&m.ExternalID, &m.Seq, &m.ParentID, &m.TS, &m.Role, &m.Model, &hash, &usage, &meta); err != nil {
			return nil, err
		}
		body, errBody := a.rebuildBody(ctx, hash)
		if errBody != nil {
			return nil, fmt.Errorf("message %s: %w", m.ExternalID, errBody)
		}
		var wrapper struct {
			Message json.RawMessage `json:"message"`
		}
		if err = json.Unmarshal(body, &wrapper); err != nil {
			return nil, err
		}
		m.Message, m.Usage, m.Metadata = wrapper.Message, usage, meta
		msgs = append(msgs, m)
	}
	if err = list.Err(); err != nil {
		return nil, err
	}
	sessions, err := a.ListSessionByID(ctx, id)
	if err != nil {
		return nil, err
	}
	srcRows, err := a.db.QueryContext(ctx, `SELECT name, blob, size, ingested_at FROM `+schema+`.session_sources
		WHERE session_id = $1 ORDER BY ingested_at`, id)
	if err != nil {
		return nil, err
	}
	defer func() { _ = srcRows.Close() }()
	sources := []map[string]any{}
	for srcRows.Next() {
		var name, blob string
		var size int64
		var at time.Time
		if err = srcRows.Scan(&name, &blob, &size, &at); err != nil {
			return nil, err
		}
		sources = append(sources, map[string]any{"name": name, "blob": blob, "size": size, "ingested_at": at})
	}
	return map[string]any{"session": sessions, "messages": msgs, "sources": sources}, nil
}

// ListSessionByID returns one session summary or an error when it doesn't exist.
func (a *Archive) ListSessionByID(ctx context.Context, id string) (*SessionSummary, error) {
	var s SessionSummary
	var meta []byte
	err := a.db.QueryRowContext(ctx, `SELECT s.id, s.harness, s.external_id, s.harness_version,
		s.machine, s.project, s.cwd, s.title, s.via_proxy, s.started_at, s.ended_at, s.metadata,
		(SELECT count(*) FROM `+schema+`.session_messages m WHERE m.session_id = s.id), s.updated_at
		FROM `+schema+`.sessions s WHERE s.id = $1`, id).Scan(&s.ID, &s.Harness, &s.ExternalID,
		&s.HarnessVersion, &s.Machine, &s.Project, &s.CWD, &s.Title, &s.ViaProxy, &s.StartedAt,
		&s.EndedAt, &meta, &s.Messages, &s.UpdatedAt)
	if err != nil {
		return nil, err
	}
	s.Metadata = meta
	return &s, nil
}
