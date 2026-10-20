package archive

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"
)

// analyticsConversation serves GET /tmx/archive/conversation from the analytics DB. ok is false
// when the session isn't extracted yet (the caller then rebuilds it from the archive).
func (an *analytics) conversation(ctx context.Context, sessionID, harness, kind string) (*Conversation, bool, error) {
	var id, k, h, title, model, traceID, ext, altJSON string
	query := func(where string, args ...any) error {
		return an.db.QueryRowContext(ctx, `SELECT id, kind, harness, title, model, trace_id, external_id,
			array_to_json(alt_ids)::text FROM sessions WHERE `+where+` ORDER BY extracted_at DESC LIMIT 1`, args...).Scan(
			&id, &k, &h, &title, &model, &traceID, &ext, &altJSON)
	}
	err := sql.ErrNoRows
	if kind != "proxy" {
		err = query(`kind = 'imported' AND (id = $1 OR external_id = $1) AND ($2 = '' OR harness = $2)`, sessionID, harness)
	}
	if errors.Is(err, sql.ErrNoRows) && kind != "imported" {
		err = query(`kind = 'proxy' AND (external_id = $1 OR $1 = ANY(alt_ids))`, sessionID)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	conv := &Conversation{Kind: k, SessionID: id, Harness: h, Title: title, Items: []ConvItem{}, Source: "analytics"}
	if k == "proxy" {
		conv.SessionID, conv.Model, conv.TraceID = sessionID, model, traceID
	}
	var total int
	if err = an.db.QueryRowContext(ctx, `SELECT count(*) FROM items WHERE session_id = $1`, id).Scan(&total); err != nil {
		return nil, false, err
	}
	// Imported transcripts show the start; proxied sessions the latest items (as the archive path does).
	offset := 0
	if total > convMaxItems {
		conv.Truncated = true
		if k == "proxy" {
			offset = total - convMaxItems
		}
	}
	rows, err := an.db.QueryContext(ctx, `SELECT role, kind, text, name, call_id, is_error, ts, model FROM items
		WHERE session_id = $1 ORDER BY seq, idx OFFSET $2 LIMIT $3`, id, offset, convMaxItems)
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var it ConvItem
		var ts sql.NullTime
		if err = rows.Scan(&it.Role, &it.Kind, &it.Text, &it.Name, &it.CallID, &it.IsError, &ts, &it.Model); err != nil {
			return nil, false, err
		}
		it.TS = nullTimePtr(ts)
		conv.Items = append(conv.Items, it)
	}
	if err = rows.Err(); err != nil {
		return nil, false, err
	}
	if k == "proxy" {
		var alt []string
		_ = json.Unmarshal([]byte(altJSON), &alt)
		keys := append([]string{ext}, alt...)
		ex, errE := an.db.QueryContext(ctx, `SELECT trace_id, ts, url, status, request_bytes FROM exchanges
			WHERE (session_id = ANY($1::text[]) OR client_session_id = ANY($1::text[])) AND method = 'POST'
			ORDER BY ts LIMIT $2`, pgArray(keys), convMaxExch)
		if errE != nil {
			return nil, false, errE
		}
		defer func() { _ = ex.Close() }()
		for ex.Next() {
			var e ConvExchange
			if errE = ex.Scan(&e.TraceID, &e.TS, &e.URL, &e.Status, &e.RequestBytes); errE != nil {
				return nil, false, errE
			}
			conv.Exchanges = append(conv.Exchanges, e)
		}
	}
	return conv, true, nil
}

// SearchQuery filters GET /tmx/analytics/search.
type SearchQuery struct {
	Q               string
	Role, Kind      string
	Name            string // tool name
	Harness         string
	Machine         string
	SessionKind     string // imported | proxy
	SessionID       string
	From, To        time.Time
	IncludeShadowed bool
	Limit           int
}

// SearchHit is one matching item with its session context.
type SearchHit struct {
	SessionID   string     `json:"session_id"`
	SessionKind string     `json:"session_kind"`
	Harness     string     `json:"harness,omitempty"`
	Machine     string     `json:"machine,omitempty"`
	Project     string     `json:"project,omitempty"`
	Title       string     `json:"title,omitempty"`
	Seq         int        `json:"seq"`
	Idx         int        `json:"idx"`
	Role        string     `json:"role"`
	Kind        string     `json:"kind"`
	Name        string     `json:"name,omitempty"`
	CallID      string     `json:"call_id,omitempty"`
	IsError     bool       `json:"is_error,omitempty"`
	TS          *time.Time `json:"ts,omitempty"`
	Snippet     string     `json:"snippet"`
}

// AnalyticsSearch finds items by substring (case-insensitive, trigram-indexed) and filters.
func (a *Archive) AnalyticsSearch(ctx context.Context, q SearchQuery) ([]SearchHit, error) {
	an := a.analytics
	if an == nil {
		return nil, ErrAnalyticsOff
	}
	if q.Limit <= 0 || q.Limit > 500 {
		q.Limit = 100
	}
	where := []string{"TRUE"}
	var args []any
	arg := func(v any) string {
		args = append(args, v)
		return "$" + strconv.Itoa(len(args))
	}
	if q.Q != "" {
		where = append(where, "i.text ILIKE "+arg("%"+escapeLike(q.Q)+"%"))
	}
	for col, v := range map[string]string{"i.role": q.Role, "i.kind": q.Kind, "i.name": q.Name, "s.harness": q.Harness,
		"s.machine": q.Machine, "s.kind": q.SessionKind, "s.id": q.SessionID} {
		if v != "" {
			where = append(where, col+" = "+arg(v))
		}
	}
	if !q.From.IsZero() {
		where = append(where, "i.ts >= "+arg(q.From))
	}
	if !q.To.IsZero() {
		where = append(where, "i.ts < "+arg(q.To))
	}
	if !q.IncludeShadowed {
		where = append(where, "s.shadowed_by IS NULL")
	}
	snippetFrom := "1"
	if q.Q != "" {
		snippetFrom = "GREATEST(1, strpos(lower(i.text), lower(" + arg(q.Q) + ")) - 120)"
	}
	sqlText := `SELECT s.id, s.kind, s.harness, s.machine, s.project, s.title, i.seq, i.idx, i.role, i.kind, i.name,
		i.call_id, i.is_error, i.ts, substr(i.text, ` + snippetFrom + `, 400)
		FROM items i JOIN sessions s ON s.id = i.session_id
		WHERE ` + strings.Join(where, " AND ") + ` ORDER BY i.ts DESC NULLS LAST LIMIT ` + arg(q.Limit)

	tx, err := an.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err = tx.ExecContext(ctx, `SET LOCAL statement_timeout = '20s'`); err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, sqlText, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	hits := []SearchHit{}
	for rows.Next() {
		var h SearchHit
		var ts sql.NullTime
		if err = rows.Scan(&h.SessionID, &h.SessionKind, &h.Harness, &h.Machine, &h.Project, &h.Title, &h.Seq, &h.Idx,
			&h.Role, &h.Kind, &h.Name, &h.CallID, &h.IsError, &ts, &h.Snippet); err != nil {
			return nil, err
		}
		h.TS = nullTimePtr(ts)
		hits = append(hits, h)
	}
	return hits, rows.Err()
}

func escapeLike(s string) string {
	return strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(s)
}
