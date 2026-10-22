package archive

// Session attachments (tokenmaxx fork): files a harness keeps next to a transcript instead of in
// it — Claude Code <session>/tool-results/*.txt and images, OpenCode tool-output/*.txt (deleted by
// OpenCode after 7 days) — each kept as a blob and indexed by (harness, session, relative path,
// sha256). A file that changes gets a second row; nothing is replaced. Analytics mirrors the
// index (session_attachments).

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// AttachmentIngest is one file. Content comes inline (data_base64) or is uploaded first with
// PUT /tmx/archive/blobs/<sha256>.
type AttachmentIngest struct {
	Harness string `json:"harness"`
	// SessionID is the harness's session id (as imported); "" when the file can't be tied to a
	// session (then ExternalPath is required).
	SessionID string `json:"session_id,omitempty"`
	// Path is relative: to the session's directory (Claude Code "tool-results/x.txt") or to the
	// harness data directory when there is no session ("tool-output/tool_x.txt").
	Path         string          `json:"path"`
	ExternalPath string          `json:"external_path,omitempty"` // absolute path on the machine
	ContentType  string          `json:"content_type,omitempty"`
	Size         int64           `json:"size"`
	SHA256       string          `json:"sha256"`
	MTime        *time.Time      `json:"mtime,omitempty"`
	DataBase64   string          `json:"data_base64,omitempty"`
	Metadata     json.RawMessage `json:"metadata,omitempty"`
}

// AttachmentBatch is the body of PUT /tmx/archive/attachments.
type AttachmentBatch struct {
	Machine     string             `json:"machine,omitempty"`
	Attachments []AttachmentIngest `json:"attachments"`
}

const (
	maxAttachmentBatch      = 1000
	maxAttachmentInline     = 8 << 20
	maxAttachmentPathLength = 4096
)

// AttachmentKey is the archive id of an attachment version.
func AttachmentKey(machine, harness, sessionID, relPath, sha string) string {
	if sessionID != "" {
		machine = "" // a session id is global; the machine only scopes session-less files
	}
	return "att_" + digestOf(machine, harness, sessionID, relPath, sha)
}

// IngestAttachments stores a batch of attachment files with per-item results.
func (a *Archive) IngestAttachments(ctx context.Context, in AttachmentBatch) (*BatchResult, error) {
	in.Machine = strings.TrimSpace(in.Machine)
	if len(in.Attachments) > maxAttachmentBatch {
		return nil, fmt.Errorf("at most %d attachments per batch", maxAttachmentBatch)
	}
	res := &BatchResult{Results: make([]ItemResult, 0, len(in.Attachments))}
	for i, at := range in.Attachments {
		res.add(a.ingestAttachment(ctx, in.Machine, i, at))
	}
	return res, nil
}

func (a *Archive) ingestAttachment(ctx context.Context, machine string, i int, at AttachmentIngest) ItemResult {
	at.Harness, at.SessionID = strings.TrimSpace(at.Harness), strings.TrimSpace(at.SessionID)
	at.Path, at.ExternalPath = strings.TrimSpace(at.Path), strings.TrimSpace(at.ExternalPath)
	at.SHA256 = strings.ToLower(strings.TrimSpace(at.SHA256))
	switch {
	case !validIdent(at.Harness, 64):
		return itemError(i, 400, "harness is required (no spaces)")
	case at.SessionID != "" && !validIdent(at.SessionID, 256):
		return itemError(i, 400, "session_id: no spaces, at most 256 characters")
	case at.Path == "" || len(at.Path) > maxAttachmentPathLength || strings.HasPrefix(at.Path, "/"):
		return itemError(i, 400, "path is required and must be relative")
	case len(at.ExternalPath) > maxAttachmentPathLength:
		return itemError(i, 400, "external_path is too long")
	case at.SessionID == "" && at.ExternalPath == "":
		return itemError(i, 400, "session_id or external_path is required")
	case at.SessionID == "" && machine == "":
		return itemError(i, 400, "machine is required for attachments without a session_id")
	case !ValidHash(at.SHA256):
		return itemError(i, 400, "sha256 must be 64 hex characters")
	case at.Size < 0:
		return itemError(i, 400, "size can't be negative")
	case len(at.Metadata) > 0 && !json.Valid(at.Metadata):
		return itemError(i, 400, "metadata is not valid JSON")
	}
	id := AttachmentKey(machine, at.Harness, at.SessionID, at.Path, at.SHA256)
	fail := func(code int, msg string) ItemResult {
		it := itemError(i, code, msg)
		it.ID = id
		return it
	}
	if at.DataBase64 != "" || (at.Size == 0 && at.SHA256 == emptySHA256) {
		data, err := base64.StdEncoding.DecodeString(at.DataBase64) // an empty file needs no upload
		if err != nil {
			return fail(400, "data_base64: "+err.Error())
		}
		if len(data) > maxAttachmentInline {
			return fail(413, fmt.Sprintf("inline data larger than %d bytes; upload the blob first", maxAttachmentInline))
		}
		if got := HashOf(data); got != at.SHA256 {
			return fail(422, "sha256 mismatch: data hashes to "+got)
		}
		if int64(len(data)) != at.Size {
			return fail(422, fmt.Sprintf("size mismatch: data is %d bytes", len(data)))
		}
		if _, err = a.blobs.Put(ctx, data, at.ContentType, "import.attachment"); err != nil {
			return fail(500, "store: "+err.Error())
		}
	} else {
		var size int64
		err := a.db.QueryRowContext(ctx, `SELECT size FROM `+schema+`.blobs WHERE hash = $1`, at.SHA256).Scan(&size)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			return ItemResult{Index: i, ID: id, Status: "missing_blob", Code: 409,
				Error: "upload the content first: PUT /tmx/archive/blobs/" + at.SHA256}
		case err != nil:
			return fail(500, "blob lookup: "+err.Error())
		case size != at.Size:
			return fail(422, fmt.Sprintf("size mismatch: blob %s is %d bytes", at.SHA256, size))
		}
	}
	var sessionKey string
	if at.SessionID != "" {
		sessionKey = SessionKey(at.Harness, at.SessionID)
	}
	insert := `INSERT INTO ` + schema + `.session_attachments (id, harness, session_id, session_key, machine,
			path, external_path, content_type, size, sha256, mtime, metadata)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
		ON CONFLICT (id) DO NOTHING RETURNING id`
	if a.analytics != nil {
		insert = `WITH ins AS (` + insert + `), j AS (INSERT INTO ` + journalTable +
			` (kind, ref) SELECT 'attachment', id FROM ins) SELECT id FROM ins`
	}
	var got string
	err := a.db.QueryRowContext(ctx, insert, id, at.Harness, at.SessionID, sessionKey, machine,
		cleanText(at.Path), cleanText(at.ExternalPath), cleanText(at.ContentType), at.Size, at.SHA256,
		timePtr(at.MTime), jsonOrEmpty(at.Metadata)).Scan(&got)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return ItemResult{Index: i, ID: id, Status: "exists", Code: 200}
	case err != nil:
		return fail(500, "index: "+err.Error())
	}
	if a.analytics != nil {
		a.analytics.poke()
	}
	return ItemResult{Index: i, ID: id, Status: "created", Code: 201}
}

// AttachmentRow is an attachment as listed.
type AttachmentRow struct {
	ID           string          `json:"id"`
	Harness      string          `json:"harness"`
	SessionID    string          `json:"session_id"`
	SessionKey   string          `json:"session_key"`
	Machine      string          `json:"machine"`
	Path         string          `json:"path"`
	ExternalPath string          `json:"external_path"`
	ContentType  string          `json:"content_type"`
	Size         int64           `json:"size"`
	SHA256       string          `json:"sha256"` // content: GET /tmx/archive/blobs/<sha256>
	MTime        *time.Time      `json:"mtime"`
	Metadata     json.RawMessage `json:"metadata"`
	IngestedAt   time.Time       `json:"ingested_at"`
}

// AttachmentQuery filters ListAttachments. SessionID is the harness's id or "<harness>:<id>".
type AttachmentQuery struct {
	Harness, SessionID, Machine, Path string
	Limit                             int
	Cursor                            string
}

// ListAttachments lists attachments, most recently ingested first, a page at a time.
func (a *Archive) ListAttachments(ctx context.Context, q AttachmentQuery) ([]AttachmentRow, string, error) {
	if q.Limit <= 0 || q.Limit > 1000 {
		q.Limit = 100
	}
	before, beforeID, err := decodeCursor(q.Cursor)
	if err != nil {
		return nil, "", err
	}
	sid := strings.TrimSpace(q.SessionID)
	rows, err := a.db.QueryContext(ctx, `SELECT id, harness, session_id, session_key, machine, path,
			external_path, content_type, size, sha256, mtime, metadata, ingested_at
		FROM `+schema+`.session_attachments
		WHERE ($1 = '' OR harness = $1) AND ($2 = '' OR session_id = $2 OR session_key = $2)
			AND ($3 = '' OR machine = $3) AND ($4 = '' OR path = $4)
			AND ($6 = '' OR (ingested_at, id) < ($7, $6))
		ORDER BY ingested_at DESC, id DESC LIMIT $5`,
		strings.TrimSpace(q.Harness), sid, strings.TrimSpace(q.Machine), strings.TrimSpace(q.Path), q.Limit+1, beforeID, before)
	if err != nil {
		return nil, "", err
	}
	defer func() { _ = rows.Close() }()
	out := []AttachmentRow{}
	for rows.Next() {
		var r AttachmentRow
		var meta []byte
		if err = rows.Scan(&r.ID, &r.Harness, &r.SessionID, &r.SessionKey, &r.Machine, &r.Path, &r.ExternalPath,
			&r.ContentType, &r.Size, &r.SHA256, &r.MTime, &meta, &r.IngestedAt); err != nil {
			return nil, "", err
		}
		r.Metadata = meta
		out = append(out, r)
	}
	if err = rows.Err(); err != nil {
		return nil, "", err
	}
	next := ""
	if len(out) > q.Limit {
		out = out[:q.Limit]
		last := out[len(out)-1]
		next = encodeCursor(last.IngestedAt, last.ID)
	}
	return out, next, nil
}
