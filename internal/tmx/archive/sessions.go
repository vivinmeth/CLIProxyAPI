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
	"github.com/tidwall/gjson"
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
	// Errors counts messages and sources that were refused; each is listed in Rejected. The rest of
	// the session is stored. A refused message keeps its stored version, if any.
	Errors   int            `json:"errors"`
	Rejected []RejectedPart `json:"rejected,omitempty"`
	// ReplaceSkipped: replace was asked for but some messages were refused, so nothing was removed
	// (the body isn't the complete session). Fix them and send again to replace.
	ReplaceSkipped bool `json:"replace_skipped,omitempty"`
}

// RejectedPart is one refused message or source of a session ingest.
type RejectedPart struct {
	Kind       string `json:"kind"`  // message | source
	Index      int    `json:"index"` // position in messages / sources
	ExternalID string `json:"external_id,omitempty"`
	Name       string `json:"name,omitempty"`
	Error      string `json:"error"`
}

func (r *IngestResult) reject(p RejectedPart) {
	r.Errors++
	r.Rejected = append(r.Rejected, p)
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
		return nil, badRequest("harness and external_id are required (no spaces, at most 64 / 256 bytes)")
	}
	if len(in.Metadata) > 0 && !json.Valid(in.Metadata) {
		return nil, badRequest("metadata is not valid JSON")
	}
	id := SessionKey(in.Harness, in.ExternalID)
	res := &IngestResult{SessionID: id, Messages: len(in.Messages)}
	// Messages without an id are keyed by position, here and in their usage keys (an empty id
	// would make every such message share "<session>#").
	for i := range in.Messages {
		in.Messages[i].ExternalID = strings.TrimSpace(in.Messages[i].ExternalID)
		if in.Messages[i].ExternalID == "" {
			in.Messages[i].ExternalID = fmt.Sprintf("seq-%d", in.Messages[i].Seq)
		}
	}

	// Raw sources must already be uploaded, so the original survives any transpiler bug. A malformed
	// source entry is refused alone.
	sources := in.Sources[:0:0]
	for i, src := range in.Sources {
		if !ValidHash(src.Blob) || strings.TrimSpace(src.Name) == "" {
			res.reject(RejectedPart{Kind: "source", Index: i, Name: src.Name, Error: "name and sha256 blob required"})
			continue
		}
		sources = append(sources, src)
	}
	in.Sources = sources
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
		return res, badRequest("upload missing source blobs first")
	}

	// Store message bodies (media extracted, deduplicated) before touching the index.
	// An invalid message is refused alone; the rest of the session is stored.
	type prepared struct {
		m     MessageIngest
		hash  string
		index int
	}
	prep := make([]prepared, 0, len(in.Messages))
	seen := map[string]bool{}
	for i, m := range in.Messages {
		if msg := validateMessage(m, seen); msg != "" {
			res.reject(RejectedPart{Kind: "message", Index: i, ExternalID: m.ExternalID, Error: msg})
			continue
		}
		seen[m.ExternalID] = true
		wrapped, _ := json.Marshal(map[string]json.RawMessage{"message": m.Message})
		hash, errSplit := a.splitBody(ctx, wrapped, "import.message")
		if errSplit != nil {
			return nil, errSplit
		}
		prep = append(prep, prepared{m: m, hash: hash, index: i})
	}

	viaProxy := in.ViaProxy
	if viaProxy == nil {
		var linked bool
		if errLink := a.db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM `+schema+`.exchanges
			WHERE client_session_id = $1 OR session_id = $1)`, in.ExternalID).Scan(&linked); errLink != nil {
			// Guessing "not proxied" would count proxied traffic twice.
			return nil, fmt.Errorf("proxied traffic lookup: %w", errLink)
		} else if linked {
			viaProxy = &linked
		}
	}
	res.ViaProxy = viaProxy
	store := requestlog.Default()
	var since *time.Time
	if viaProxy != nil && *viaProxy {
		if since, err = a.proxiedSince(ctx, store, in.ExternalID); err != nil {
			return nil, fmt.Errorf("proxied traffic lookup: %w", err)
		}
	}
	usageInTx := a.usageInArchiveDB(ctx, store)

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

	stored := make([]MessageIngest, 0, len(prep))
	for _, p := range prep {
		// The upsert says what happened: a new row, a changed one (body, position, usage or
		// metadata), or nothing (no row returned). A message the database refuses is rolled back
		// alone (savepoint).
		if _, err = tx.ExecContext(ctx, `SAVEPOINT tmx_message`); err != nil {
			return nil, err
		}
		var inserted bool
		errUp := tx.QueryRowContext(ctx, `INSERT INTO `+schema+`.session_messages (session_id,
			external_id, seq, parent_id, ts, role, model, message, usage, metadata)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
			ON CONFLICT (session_id, external_id) DO UPDATE SET seq = EXCLUDED.seq,
				parent_id = EXCLUDED.parent_id, ts = EXCLUDED.ts, role = EXCLUDED.role,
				model = EXCLUDED.model, message = EXCLUDED.message, usage = EXCLUDED.usage,
				metadata = EXCLUDED.metadata, ingested_at = now()
			WHERE session_messages.message IS DISTINCT FROM EXCLUDED.message
				OR session_messages.seq IS DISTINCT FROM EXCLUDED.seq
				OR session_messages.usage IS DISTINCT FROM EXCLUDED.usage
								OR session_messages.metadata IS DISTINCT FROM EXCLUDED.metadata
			RETURNING (xmax = 0)`,
			id, p.m.ExternalID, p.m.Seq, p.m.ParentID, timePtr(p.m.TS), p.m.Role, p.m.Model, p.hash,
			nullJSON(p.m.Usage), rawOrEmpty(p.m.Metadata, "{}")).Scan(&inserted)
		switch {
		case errors.Is(errUp, sql.ErrNoRows):
			res.Unchanged++
		case errUp != nil:
			if _, err = tx.ExecContext(ctx, `ROLLBACK TO SAVEPOINT tmx_message`); err != nil {
				return nil, err
			}
			if !isDataError(errUp) {
				return nil, fmt.Errorf("message %q: %w", p.m.ExternalID, errUp)
			}
			res.reject(RejectedPart{Kind: "message", Index: p.index, ExternalID: p.m.ExternalID, Error: errUp.Error()})
			continue
		case inserted:
			res.Inserted++
		default:
			res.Updated++
		}
		if _, err = tx.ExecContext(ctx, `RELEASE SAVEPOINT tmx_message`); err != nil {
			return nil, err
		}
		stored = append(stored, p.m)
	}
	// Usage rows follow the stored messages only. With a refused message the body isn't the whole
	// session, so replace removes nothing (neither messages nor usage rows).
	in.Messages = stored
	if in.Replace && res.Errors > 0 {
		in.Replace, res.ReplaceSkipped = false, true
	}
	if in.Replace {
		keep := make([]string, 0, len(stored))
		for _, m := range stored {
			keep = append(keep, m.ExternalID)
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
	// Usage rows go to tmx.requests (source extern:<harness>) so cost views can include them; in
	// the same transaction when it shares the database, so archive and usage never drift.
	if usageInTx {
		if err = a.writeUsage(ctx, tx, in, id, viaProxy, since, res); err != nil {
			return nil, err
		}
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	if !usageInTx && store != nil {
		if err = a.writeUsage(ctx, store.DB(), in, id, viaProxy, since, res); err != nil {
			return res, err
		}
	}
	return res, nil
}

// validateMessage returns why a message can't be stored, or "". seen holds the ids already taken.
func validateMessage(m MessageIngest, seen map[string]bool) string {
	switch {
	case seen[m.ExternalID]:
		return fmt.Sprintf("duplicate external_id %q", m.ExternalID)
	case !json.Valid(m.Message):
		return "message is not valid JSON"
	case (len(m.Usage) > 0 && !json.Valid(m.Usage)) || (len(m.Metadata) > 0 && !json.Valid(m.Metadata)):
		return "usage/metadata is not valid JSON"
	}
	if b := gjson.GetBytes(m.Usage, "billing").String(); !requestlog.ValidBilling(strings.TrimSpace(b)) {
		return "billing must be subscription, api_key, local or free"
	}
	return ""
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
	store := requestlog.Default()
	usageInTx := a.usageInArchiveDB(ctx, store)
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
	// Usage rows go with the session, in the same transaction when they share the database.
	if usageInTx {
		if res.UsageRows, err = store.DeleteImportedWith(ctx, tx, id+"#", nil); err != nil {
			return nil, fmt.Errorf("usage rows: %w", err)
		}
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	if !usageInTx {
		if res.UsageRows, err = store.DeleteImported(ctx, id+"#", nil); err != nil {
			return res, fmt.Errorf("usage rows: %w", err)
		}
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

// ListSessions lists ingested sessions, most recently updated first, a page at a time: pass the
// returned next cursor back to get the following page ("" = no more). Paging is by
// (updated_at, id), so a session re-imported while paging moves to the front instead of shifting
// the pages after it.
func (a *Archive) ListSessions(ctx context.Context, harness, machine string, since time.Time, limit int,
	cursor string) ([]SessionSummary, string, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	before, beforeID, err := decodeCursor(cursor)
	if err != nil {
		return nil, "", err
	}
	rows, err := a.db.QueryContext(ctx, `SELECT s.id, s.harness, s.external_id, s.harness_version,
		s.machine, s.project, s.cwd, s.title, s.via_proxy, s.started_at, s.ended_at, s.metadata,
		(SELECT count(*) FROM `+schema+`.session_messages m WHERE m.session_id = s.id), s.updated_at
		FROM `+schema+`.sessions s
		WHERE ($1 = '' OR s.harness = $1) AND ($2 = '' OR s.machine = $2) AND s.updated_at >= $3
			AND ($5 = '' OR (s.updated_at, s.id) < ($6, $5))
		ORDER BY s.updated_at DESC, s.id DESC LIMIT $4`, harness, machine, since, limit+1, beforeID, before)
	if err != nil {
		return nil, "", err
	}
	defer func() { _ = rows.Close() }()
	out := []SessionSummary{}
	for rows.Next() {
		var s SessionSummary
		var meta []byte
		if err = rows.Scan(&s.ID, &s.Harness, &s.ExternalID, &s.HarnessVersion, &s.Machine, &s.Project,
			&s.CWD, &s.Title, &s.ViaProxy, &s.StartedAt, &s.EndedAt, &meta, &s.Messages, &s.UpdatedAt); err != nil {
			return nil, "", err
		}
		s.Metadata = meta
		out = append(out, s)
	}
	if err = rows.Err(); err != nil {
		return nil, "", err
	}
	next := ""
	if len(out) > limit {
		out = out[:limit]
		last := out[len(out)-1]
		next = encodeCursor(last.UpdatedAt, last.ID)
	}
	return out, next, nil
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
	if err = srcRows.Err(); err != nil {
		return nil, err
	}
	// Raw files as they stand now (whole uploads and appends) and files kept next to the transcript.
	files, err := a.ListFiles(ctx, id)
	if err != nil {
		return nil, err
	}
	attachments, _, err := a.ListAttachments(ctx, AttachmentQuery{SessionID: id, Limit: 1000})
	if err != nil {
		return nil, err
	}
	return map[string]any{"session": sessions, "messages": msgs, "sources": sources, "files": files,
		"attachments": attachments}, nil
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
