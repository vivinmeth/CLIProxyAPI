package archive

// Prompt history (tokenmaxx fork): the harnesses' own prompt logs — Claude Code
// ~/.claude/history.jsonl, Codex ~/.codex/history.jsonl — one line per prompt. For some machines
// these files are the only record of a year of prompts (their transcripts were cleaned up), so
// every line is kept verbatim as its own blob and indexed (machine, harness, time, project/cwd,
// session id, text hash). Lines are deduplicated per (machine, harness, line hash), so re-sending
// the whole file every run is harmless. Analytics extracts them into prompt_history.

import (
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/tidwall/gjson"
)

// PromptLine is one line of a prompt history file. Raw is the line exactly as in the file without
// its "\n" terminator; RawBase64 carries the same bytes base64-encoded for lines that aren't
// valid UTF-8 (one of the two is required).
type PromptLine struct {
	Raw       *string `json:"raw,omitempty"`
	RawBase64 string  `json:"raw_base64,omitempty"`
	LineNo    int64   `json:"line_no,omitempty"` // 1-based line number in the file, informational
}

// PromptBatch is the body of PUT /tmx/archive/prompts: lines of one history file.
type PromptBatch struct {
	Machine string       `json:"machine"`
	Harness string       `json:"harness"`        // claude-code | codex | ...
	File    string       `json:"file,omitempty"` // path of the history file on the machine
	Lines   []PromptLine `json:"lines"`
}

// ItemResult is the per-item outcome of a batch ingest (prompts, attachments, file appends).
type ItemResult struct {
	Index  int    `json:"index"`
	Status string `json:"status"` // created | exists | appended | conflict | missing_blob | error
	Code   int    `json:"code"`   // HTTP-style: 200 exists/appended, 201 created, 400/409/413/422/500
	ID     string `json:"id,omitempty"`
	Error  string `json:"error,omitempty"`
	// Retryable: the same item may succeed if sent again unchanged (a server-side failure).
	Retryable bool   `json:"retryable,omitempty"`
	Warning   string `json:"warning,omitempty"`
	LineHash  string `json:"line_hash,omitempty"` // prompts: sha256 of the raw line (= its blob)
	// File state after the item (file appends; on conflict: what the server has).
	File *FileState `json:"file,omitempty"`
}

// BatchResult is the response of a batch ingest.
type BatchResult struct {
	Results   []ItemResult `json:"results"`
	Created   int          `json:"created"`
	Exists    int          `json:"exists"`
	Appended  int          `json:"appended,omitempty"`
	Conflicts int          `json:"conflicts"`
	Errors    int          `json:"errors"`
}

func (r *BatchResult) add(it ItemResult) {
	switch it.Status {
	case "created":
		r.Created++
	case "exists":
		r.Exists++
	case "appended":
		r.Appended++
	case "conflict", "missing_blob":
		r.Conflicts++
	default:
		r.Errors++
	}
	r.Results = append(r.Results, it)
}

// HTTPStatus summarises a batch: 200 when every item went through, 409 when the only failures
// are conflicts (file offset mismatch, blob not uploaded yet), 207 for any other per-item error.
// The per-item results are always the authority.
func (r *BatchResult) HTTPStatus() int {
	switch {
	case r.Errors > 0:
		return 207
	case r.Conflicts > 0:
		return 409
	}
	return 200
}

func itemError(i, code int, err string) ItemResult {
	return ItemResult{Index: i, Status: "error", Code: code, Error: err, Retryable: code >= 500}
}

const (
	maxPromptBatch     = 5000
	maxPromptLineBytes = 8 << 20
)

// PromptKey is the archive id of a prompt line: one per (machine, harness, line hash).
func PromptKey(machine, harness, lineHash string) string {
	return "p_" + digestOf(machine, harness, lineHash)
}

// parsedPrompt is what the index keeps of a line.
type parsedPrompt struct {
	ts                     *time.Time
	project, cwd, session  string
	text                   string
	textHash, parseWarning string
	hasText                bool
}

// parsePromptLine reads the fields of a history line. Claude Code: {display, pastedContents,
// timestamp (unix ms), project (cwd), sessionId}. Codex: {session_id, ts (unix s), text}. Other
// harnesses: the same names, or prompt / cwd / time. Never fails: what it can't read stays in the
// raw line.
func parsePromptLine(raw []byte) parsedPrompt {
	var p parsedPrompt
	if !gjson.ValidBytes(raw) {
		p.parseWarning = "line is not JSON; stored raw only"
		return p
	}
	r := gjson.ParseBytes(raw)
	if !r.IsObject() {
		p.parseWarning = "line is not a JSON object; stored raw only"
		return p
	}
	for _, k := range []string{"timestamp", "ts", "time", "created_at"} {
		if v := r.Get(k); v.Exists() {
			p.ts = promptTime(v)
			if p.ts != nil {
				break
			}
		}
	}
	for _, k := range []string{"sessionId", "session_id", "sessionID"} {
		if v := r.Get(k); v.Type == gjson.String && strings.TrimSpace(v.Str) != "" {
			p.session = strings.TrimSpace(v.Str)
			break
		}
	}
	for _, k := range []string{"cwd", "project"} {
		if v := r.Get(k); v.Type == gjson.String && strings.TrimSpace(v.Str) != "" {
			p.cwd = strings.TrimSpace(v.Str)
			break
		}
	}
	if p.cwd != "" {
		p.project = path.Base(strings.TrimRight(p.cwd, "/"))
		if p.project == "." || p.project == "/" {
			p.project = ""
		}
	}
	for _, k := range []string{"display", "text", "prompt"} {
		if v := r.Get(k); v.Type == gjson.String {
			p.text, p.hasText = v.Str, true
			break
		}
	}
	if p.hasText {
		p.textHash = HashOf([]byte(p.text))
	} else {
		p.parseWarning = "no prompt text field found; stored raw"
	}
	return p
}

// promptTime reads a unix time in seconds or milliseconds, or an RFC 3339 string.
func promptTime(v gjson.Result) *time.Time {
	var t time.Time
	switch v.Type {
	case gjson.Number:
		f := v.Float()
		switch {
		case f <= 0:
			return nil
		case f > 1e15: // microseconds
			t = time.UnixMicro(int64(f))
		case f > 1e11: // milliseconds
			t = time.UnixMilli(int64(f))
		default:
			t = time.Unix(int64(f), int64((f-float64(int64(f)))*1e9))
		}
	case gjson.String:
		s := strings.TrimSpace(v.Str)
		if n, err := strconv.ParseFloat(s, 64); err == nil {
			return promptTime(gjson.Parse(strconv.FormatFloat(n, 'f', -1, 64)))
		}
		var err error
		if t, err = time.Parse(time.RFC3339Nano, s); err != nil {
			return nil
		}
	default:
		return nil
	}
	t = t.UTC()
	return &t
}

// IngestPrompts stores a batch of history lines: each raw line as a blob plus an index row.
// Per-line results; a bad line never stops the others. Idempotent: a line already stored for
// the machine and harness is "exists".
func (a *Archive) IngestPrompts(ctx context.Context, in PromptBatch) (*BatchResult, error) {
	in.Machine, in.Harness = strings.TrimSpace(in.Machine), strings.TrimSpace(in.Harness)
	if !validIdent(in.Harness, 64) {
		return nil, fmt.Errorf("harness is required (no spaces)")
	}
	if !validIdent(in.Machine, 128) {
		return nil, fmt.Errorf("machine is required (no spaces)")
	}
	if len(in.Lines) > maxPromptBatch {
		return nil, fmt.Errorf("at most %d lines per batch", maxPromptBatch)
	}
	res := &BatchResult{Results: make([]ItemResult, 0, len(in.Lines))}
	for i, l := range in.Lines {
		res.add(a.ingestPromptLine(ctx, in, i, l))
	}
	return res, nil
}

func (a *Archive) ingestPromptLine(ctx context.Context, in PromptBatch, i int, l PromptLine) ItemResult {
	var raw []byte
	switch {
	case l.Raw != nil && l.RawBase64 != "":
		return itemError(i, 400, "send raw or raw_base64, not both")
	case l.Raw != nil:
		raw = []byte(*l.Raw)
	case l.RawBase64 != "":
		var err error
		if raw, err = base64.StdEncoding.DecodeString(l.RawBase64); err != nil {
			return itemError(i, 400, "raw_base64: "+err.Error())
		}
	default:
		return itemError(i, 400, "raw or raw_base64 is required")
	}
	if len(raw) == 0 {
		return itemError(i, 400, "empty line")
	}
	if len(raw) > maxPromptLineBytes {
		return itemError(i, 413, fmt.Sprintf("line larger than %d bytes", maxPromptLineBytes))
	}
	if strings.IndexByte(string(raw), '\n') >= 0 {
		return itemError(i, 400, "line contains a newline; send one line per item, without its terminator")
	}
	hash := HashOf(raw)
	id := PromptKey(in.Machine, in.Harness, hash)
	p := parsePromptLine(raw)
	// The raw line first: an index row never points at a blob that isn't stored.
	if _, err := a.blobs.Put(ctx, raw, "application/json", "import.prompt"); err != nil {
		it := itemError(i, 500, "store line: "+err.Error())
		it.ID, it.LineHash = id, hash
		return it
	}
	var sessionKey string
	if p.session != "" {
		sessionKey = SessionKey(in.Harness, p.session)
	}
	insert := `INSERT INTO ` + schema + `.prompt_history (id, machine, harness, line_hash, ts, project, cwd,
			session_id, session_key, text_hash, text_bytes, source_file, line_no, parse_warning)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
		ON CONFLICT (machine, harness, line_hash) DO NOTHING RETURNING id`
	if a.analytics != nil {
		// One statement: the journal entry exists exactly when the row does.
		insert = `WITH ins AS (` + insert + `), j AS (INSERT INTO ` + journalTable +
			` (kind, ref) SELECT 'prompt', id FROM ins) SELECT id FROM ins`
	}
	var got string
	err := a.db.QueryRowContext(ctx, insert, id, in.Machine, in.Harness, hash, timePtr(p.ts),
		cleanText(p.project), cleanText(p.cwd), cleanText(p.session), cleanText(sessionKey), p.textHash,
		len(p.text), cleanText(in.File), l.LineNo, p.parseWarning).Scan(&got)
	it := ItemResult{Index: i, ID: id, LineHash: hash, Warning: p.parseWarning}
	switch {
	case errors.Is(err, sql.ErrNoRows):
		it.Status, it.Code = "exists", 200
	case err != nil:
		it = itemError(i, 500, "index: "+err.Error())
		it.ID, it.LineHash = id, hash
	default:
		it.Status, it.Code = "created", 201
		if a.analytics != nil {
			a.analytics.poke()
		}
	}
	return it
}

// PromptQuery filters ListPrompts.
type PromptQuery struct {
	Machine, Harness string
	SessionID        string // the harness's id, or "<harness>:<id>"
	Since, Until     time.Time
	Limit            int
	Cursor           string
}

// PromptRow is a prompt index row as listed.
type PromptRow struct {
	ID           string     `json:"id"`
	Machine      string     `json:"machine"`
	Harness      string     `json:"harness"`
	LineHash     string     `json:"line_hash"` // the raw line: GET /tmx/archive/blobs/<line_hash>
	TS           *time.Time `json:"ts"`
	Project      string     `json:"project"`
	CWD          string     `json:"cwd"`
	SessionID    string     `json:"session_id"`
	SessionKey   string     `json:"session_key"` // "<harness>:<session_id>", the archive session id
	TextHash     string     `json:"text_hash"`
	TextBytes    int64      `json:"text_bytes"`
	SourceFile   string     `json:"source_file"`
	LineNo       int64      `json:"line_no"`
	ParseWarning string     `json:"parse_warning,omitempty"`
	IngestedAt   time.Time  `json:"ingested_at"`
	Text         *string    `json:"text,omitempty"` // with include=text
}

// ListPrompts lists indexed prompt lines, newest first (by ts, then id), a page at a time.
// withText reads each raw line back and adds its text.
func (a *Archive) ListPrompts(ctx context.Context, q PromptQuery, withText bool) ([]PromptRow, string, error) {
	if q.Limit <= 0 || q.Limit > 1000 {
		q.Limit = 100
	}
	before, beforeID, err := decodeCursor(q.Cursor)
	if err != nil {
		return nil, "", err
	}
	until := q.Until
	if until.IsZero() {
		until = time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	sid := strings.TrimSpace(q.SessionID)
	rows, err := a.db.QueryContext(ctx, `SELECT id, machine, harness, line_hash, ts, project, cwd, session_id,
			session_key, text_hash, text_bytes, source_file, line_no, parse_warning, ingested_at,
			COALESCE(ts, 'epoch')
		FROM `+schema+`.prompt_history
		WHERE ($1 = '' OR machine = $1) AND ($2 = '' OR harness = $2)
			AND ($3 = '' OR session_id = $3 OR session_key = $3)
			AND COALESCE(ts, 'epoch') >= $4 AND COALESCE(ts, 'epoch') < $5
			AND ($7 = '' OR (COALESCE(ts, 'epoch'), id) < ($8, $7))
		ORDER BY COALESCE(ts, 'epoch') DESC, id DESC LIMIT $6`,
		q.Machine, q.Harness, sid, q.Since, until, q.Limit+1, beforeID, before)
	if err != nil {
		return nil, "", err
	}
	defer func() { _ = rows.Close() }()
	out := []PromptRow{}
	var lastAt time.Time
	next := ""
	for rows.Next() {
		var r PromptRow
		var at time.Time
		if err = rows.Scan(&r.ID, &r.Machine, &r.Harness, &r.LineHash, &r.TS, &r.Project, &r.CWD, &r.SessionID,
			&r.SessionKey, &r.TextHash, &r.TextBytes, &r.SourceFile, &r.LineNo, &r.ParseWarning, &r.IngestedAt, &at); err != nil {
			return nil, "", err
		}
		if len(out) == q.Limit {
			next = encodeCursor(lastAt, out[len(out)-1].ID)
			break
		}
		out = append(out, r)
		lastAt = at
	}
	if err = rows.Err(); err != nil {
		return nil, "", err
	}
	if withText {
		hashes := make([]string, len(out))
		for i := range out {
			hashes[i] = out[i].LineHash
		}
		a.blobs.Prefetch(ctx, hashes)
		for i := range out {
			raw, errGet := a.blobs.Get(ctx, out[i].LineHash)
			if errGet != nil {
				return nil, "", fmt.Errorf("prompt %s: %w", out[i].ID, errGet)
			}
			if p := parsePromptLine(raw); p.hasText {
				out[i].Text = &p.text
			}
		}
	}
	return out, next, nil
}
