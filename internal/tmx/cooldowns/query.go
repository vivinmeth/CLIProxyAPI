package cooldowns

import (
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	DefaultLimit = 500
	MaxLimit     = 5000
)

// ErrBadCursor is returned for a cursor that doesn't decode.
var ErrBadCursor = errors.New("invalid cursor")

// Query selects events (newest first) and the windows overlapping [From, To).
type Query struct {
	From, To time.Time
	AuthID   string
	Model    string
	Kinds    []string
	Sources  []string
	Cursor   string // from a previous page's next_cursor
	Limit    int
}

// Window is one cooldown, paired from its start and end events. EndedAt is nil while open.
type Window struct {
	CooldownID   string     `json:"cooldown_id"`
	AuthID       string     `json:"auth_id"`
	AuthIndex    string     `json:"auth_index,omitempty"`
	AuthLabel    string     `json:"auth_label,omitempty"`
	Provider     string     `json:"provider,omitempty"`
	Model        string     `json:"model"`
	Reason       string     `json:"reason,omitempty"`
	HTTPStatus   int        `json:"http_status,omitempty"`
	BackoffLevel int        `json:"backoff_level,omitempty"`
	StartedAt    time.Time  `json:"started_at"`
	RetryAt      *time.Time `json:"retry_at,omitempty"`
	EndedAt      *time.Time `json:"ended_at"`
	EndReason    string     `json:"end_reason,omitempty"`
	DurationMs   int64      `json:"duration_ms"`
	Open         bool       `json:"open"`
	Updates      int        `json:"updates"`
	Source       string     `json:"source"`
}

// Page is one page of history.
type Page struct {
	Events     []Event  `json:"events"`
	Windows    []Window `json:"windows"`
	NextCursor string   `json:"next_cursor"`
	Truncated  bool     `json:"windows_truncated"`
}

func encodeCursor(ts time.Time, id int64) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.FormatInt(ts.UnixMicro(), 10) + ":" + strconv.FormatInt(id, 10)))
}

func decodeCursor(raw string) (time.Time, int64, error) {
	b, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return time.Time{}, 0, ErrBadCursor
	}
	tsRaw, idRaw, ok := strings.Cut(string(b), ":")
	if !ok {
		return time.Time{}, 0, ErrBadCursor
	}
	us, err1 := strconv.ParseInt(tsRaw, 10, 64)
	id, err2 := strconv.ParseInt(idRaw, 10, 64)
	if err1 != nil || err2 != nil {
		return time.Time{}, 0, ErrBadCursor
	}
	return time.UnixMicro(us).UTC(), id, nil
}

// History reads one page of events and, on the first page only, the windows.
func (s *Store) History(ctx context.Context, q Query) (Page, error) {
	if s == nil {
		return Page{Events: []Event{}, Windows: []Window{}}, nil
	}
	return History(ctx, s.db, s.schema, q)
}

// History runs q against schema.cooldown_events on db.
func History(ctx context.Context, db *sql.DB, schema string, q Query) (Page, error) {
	if q.Limit <= 0 {
		q.Limit = DefaultLimit
	}
	q.Limit = min(q.Limit, MaxLimit)
	table := schema + ".cooldown_events"
	page := Page{Events: []Event{}, Windows: []Window{}}

	where := []string{"ts >= $1", "ts < $2"}
	args := []any{q.From, q.To}
	add := func(cond string, v any) {
		args = append(args, v)
		where = append(where, fmt.Sprintf(cond, len(args)))
	}
	if q.AuthID != "" {
		add("auth_id = $%d", q.AuthID)
	}
	if q.Model != "" {
		add("model = $%d", q.Model)
	}
	if len(q.Kinds) > 0 {
		add("kind = ANY($%d)", q.Kinds)
	}
	if len(q.Sources) > 0 {
		add("source = ANY($%d)", q.Sources)
	}
	if q.Cursor != "" {
		ts, id, err := decodeCursor(q.Cursor)
		if err != nil {
			return page, err
		}
		args = append(args, ts, id)
		where = append(where, fmt.Sprintf("(ts, id) < ($%d, $%d)", len(args)-1, len(args)))
	}
	args = append(args, q.Limit+1)
	rows, err := db.QueryContext(ctx, `SELECT id, event_key, ts, recorded_at, kind, source, cooldown_id, auth_id,
		auth_index, auth_label, provider, model, reason, http_status, error_code, message, retry_at,
		retry_after_ms, backoff_level, credential_scope, started_at, ended_at, end_reason
		FROM `+table+` WHERE `+strings.Join(where, " AND ")+` ORDER BY ts DESC, id DESC LIMIT $`+strconv.Itoa(len(args)), args...)
	if err != nil {
		return page, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var e Event
		var recorded time.Time
		var retry, started, ended sql.NullTime
		if err = rows.Scan(&e.ID, &e.EventKey, &e.TS, &recorded, &e.Kind, &e.Source, &e.CooldownID, &e.AuthID,
			&e.AuthIndex, &e.AuthLabel, &e.Provider, &e.Model, &e.Reason, &e.HTTPStatus, &e.ErrorCode, &e.Message,
			&retry, &e.RetryAfterMs, &e.BackoffLevel, &e.CredScope, &started, &ended, &e.EndReason); err != nil {
			return page, err
		}
		e.TS = e.TS.UTC()
		e.RecordedAt = timePtr(recorded)
		e.RetryAt = nullTime(retry)
		e.StartedAt = nullTime(started)
		e.EndedAt = nullTime(ended)
		page.Events = append(page.Events, e)
	}
	if err = rows.Err(); err != nil {
		return page, err
	}
	if len(page.Events) > q.Limit {
		page.Events = page.Events[:q.Limit]
		last := page.Events[len(page.Events)-1]
		page.NextCursor = encodeCursor(last.TS, last.ID)
	}
	if q.Cursor == "" {
		page.Windows, page.Truncated, err = windows(ctx, db, table, q)
	}
	return page, err
}

// windows pairs start and end events into windows overlapping [From, To), oldest first.
func windows(ctx context.Context, db *sql.DB, table string, q Query) ([]Window, bool, error) {
	where := []string{"st.kind = 'cooldown_start'", "st.cooldown_id <> ''", "COALESCE(st.started_at, st.ts) < $2",
		"(en.ended_at IS NULL OR en.ended_at >= $1)"}
	args := []any{q.From, q.To}
	if q.AuthID != "" {
		args = append(args, q.AuthID)
		where = append(where, fmt.Sprintf("st.auth_id = $%d", len(args)))
	}
	if q.Model != "" {
		args = append(args, q.Model)
		where = append(where, fmt.Sprintf("st.model = $%d", len(args)))
	}
	limit := MaxLimit
	args = append(args, limit+1)
	rows, err := db.QueryContext(ctx, `SELECT st.cooldown_id, st.auth_id, st.auth_index, st.auth_label, st.provider,
		st.model, COALESCE(u.reason, st.reason), COALESCE(u.http_status, st.http_status),
		COALESCE(u.backoff_level, st.backoff_level), COALESCE(st.started_at, st.ts),
		COALESCE(u.retry_at, st.retry_at), en.ended_at, COALESCE(en.end_reason, ''), COALESCE(u.n, 0), st.source
		FROM `+table+` st
		LEFT JOIN LATERAL (SELECT ended_at, end_reason FROM `+table+` e
			WHERE e.cooldown_id = st.cooldown_id AND e.kind = 'cooldown_end' ORDER BY e.id LIMIT 1) en ON TRUE
		LEFT JOIN LATERAL (SELECT reason, http_status, backoff_level, retry_at, count(*) OVER () AS n FROM `+table+` x
			WHERE x.cooldown_id = st.cooldown_id AND x.kind = 'cooldown_update' ORDER BY x.ts DESC, x.id DESC LIMIT 1) u ON TRUE
		WHERE `+strings.Join(where, " AND ")+`
		ORDER BY COALESCE(st.started_at, st.ts), st.id LIMIT $`+strconv.Itoa(len(args)), args...)
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = rows.Close() }()
	out := []Window{}
	now := time.Now()
	for rows.Next() {
		var w Window
		var retry, ended sql.NullTime
		if err = rows.Scan(&w.CooldownID, &w.AuthID, &w.AuthIndex, &w.AuthLabel, &w.Provider, &w.Model, &w.Reason,
			&w.HTTPStatus, &w.BackoffLevel, &w.StartedAt, &retry, &ended, &w.EndReason, &w.Updates, &w.Source); err != nil {
			return nil, false, err
		}
		w.StartedAt = w.StartedAt.UTC()
		w.RetryAt = nullTime(retry)
		w.EndedAt = nullTime(ended)
		w.Open = w.EndedAt == nil
		end := now
		if w.EndedAt != nil {
			end = *w.EndedAt
		}
		w.DurationMs = max(int64(0), end.Sub(w.StartedAt).Milliseconds())
		out = append(out, w)
	}
	if err = rows.Err(); err != nil {
		return nil, false, err
	}
	if len(out) > limit {
		return out[:limit], true, nil
	}
	return out, false, nil
}

func nullTime(t sql.NullTime) *time.Time {
	if !t.Valid {
		return nil
	}
	return timePtr(t.Time)
}
