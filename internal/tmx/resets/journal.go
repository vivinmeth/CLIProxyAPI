package resets

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// HistoryQuery filters the claim journal. Zero values match everything; BeforeID pages (claims
// with a smaller id), Limit is applied by Service.History.
type HistoryQuery struct {
	AuthID   string
	Provider string
	Outcome  string
	State    string
	From     time.Time
	To       time.Time
	BeforeID int64
	Limit    int
}

func (q HistoryQuery) matches(r Redemption) bool {
	return (q.AuthID == "" || r.AuthID == q.AuthID) &&
		(q.Provider == "" || r.Provider == q.Provider) &&
		(q.Outcome == "" || r.Outcome == q.Outcome) &&
		(q.State == "" || r.State == q.State) &&
		(q.From.IsZero() || !r.CreatedAt.Before(q.From)) &&
		(q.To.IsZero() || r.CreatedAt.Before(q.To)) &&
		(q.BeforeID <= 0 || r.ID < q.BeforeID)
}

// journal records every claim before it is sent, so a retry reuses its request id.
type journal interface {
	// pending returns the newest unsettled claim for account+credit created after since.
	pending(ctx context.Context, authID, creditID string, since time.Time) (*Redemption, error)
	// unsettled returns every unsettled claim created after since, newest first.
	unsettled(ctx context.Context, since time.Time) ([]Redemption, error)
	insert(ctx context.Context, r *Redemption) error
	settle(ctx context.Context, r *Redemption) error
	// list returns claims matching q, newest first, at most q.Limit.
	list(ctx context.Context, q HistoryQuery) ([]Redemption, error)
}

// ---- memory (tests, or no request-log database) ----

type memJournal struct {
	mu   sync.Mutex
	next int64
	rows []Redemption
}

func newMemJournal() *memJournal { return &memJournal{} }

func (m *memJournal) pending(_ context.Context, authID, creditID string, since time.Time) (*Redemption, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := len(m.rows) - 1; i >= 0; i-- {
		r := m.rows[i]
		if r.AuthID == authID && r.CreditID == creditID && r.State == "pending" && r.CreatedAt.After(since) {
			return &r, nil
		}
	}
	return nil, nil
}

func (m *memJournal) unsettled(_ context.Context, since time.Time) ([]Redemption, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Redemption
	for i := len(m.rows) - 1; i >= 0; i-- {
		if r := m.rows[i]; r.State == "pending" && r.CreatedAt.After(since) {
			out = append(out, r)
		}
	}
	return out, nil
}

func (m *memJournal) insert(_ context.Context, r *Redemption) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.next++
	r.ID = m.next
	m.rows = append(m.rows, *r)
	return nil
}

func (m *memJournal) settle(_ context.Context, r *Redemption) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range m.rows {
		if m.rows[i].ID == r.ID {
			m.rows[i] = *r
			return nil
		}
	}
	return fmt.Errorf("claim %d not found", r.ID)
}

func (m *memJournal) list(_ context.Context, q HistoryQuery) ([]Redemption, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Redemption
	for _, r := range m.rows {
		if q.matches(r) {
			out = append(out, r)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].ID > out[j].ID })
	if q.Limit > 0 && len(out) > q.Limit {
		out = out[:q.Limit]
	}
	return out, nil
}

// ---- Postgres ----

type pgJournal struct {
	db    *sql.DB
	table string
}

func newPGJournal(ctx context.Context, db *sql.DB, schema string) (*pgJournal, error) {
	j := &pgJournal{db: db, table: schema + ".reset_redemptions"}
	for _, stmt := range []string{
		`CREATE TABLE IF NOT EXISTS ` + j.table + ` (
			id           BIGSERIAL PRIMARY KEY,
			created_at   TIMESTAMPTZ NOT NULL,
			settled_at   TIMESTAMPTZ,
			auth_id      TEXT NOT NULL,
			provider     TEXT NOT NULL,
			label        TEXT NOT NULL DEFAULT '',
			credit_id    TEXT NOT NULL,
			credit_title TEXT NOT NULL DEFAULT '',
			request_id   TEXT NOT NULL,
			state        TEXT NOT NULL,
			outcome      TEXT NOT NULL DEFAULT '',
			confirmed    BOOLEAN NOT NULL DEFAULT FALSE,
			warning      TEXT NOT NULL DEFAULT '',
			before       JSONB,
			after        JSONB,
			actor        TEXT NOT NULL DEFAULT ''
		)`,
		// The credit's scope (windows it clears), so a retried claim releases only those. Older
		// rows keep NULL: unknown scope.
		`ALTER TABLE ` + j.table + ` ADD COLUMN IF NOT EXISTS clears JSONB`,
		`CREATE INDEX IF NOT EXISTS reset_redemptions_auth_credit ON ` + j.table + ` (auth_id, credit_id, created_at)`,
		`CREATE INDEX IF NOT EXISTS reset_redemptions_created ON ` + j.table + ` (created_at)`,
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return nil, fmt.Errorf("resets schema: %w", err)
		}
	}
	return j, nil
}

const pgColumns = `id, created_at, settled_at, auth_id, provider, label, credit_id, credit_title,
	request_id, state, outcome, confirmed, warning, before, after, actor, clears`

func scanRedemption(row interface{ Scan(...any) error }) (Redemption, error) {
	var r Redemption
	var settled sql.NullTime
	var before, after, clears []byte
	err := row.Scan(&r.ID, &r.CreatedAt, &settled, &r.AuthID, &r.Provider, &r.Label, &r.CreditID,
		&r.CreditTitle, &r.RequestID, &r.State, &r.Outcome, &r.Confirmed, &r.Warning, &before, &after, &r.Actor, &clears)
	if err != nil {
		return r, err
	}
	if settled.Valid {
		t := settled.Time.UTC()
		r.SettledAt = &t
	}
	r.CreatedAt = r.CreatedAt.UTC()
	r.Before, r.After = []Window{}, []Window{}
	if len(before) > 0 {
		_ = json.Unmarshal(before, &r.Before)
	}
	if len(after) > 0 {
		_ = json.Unmarshal(after, &r.After)
	}
	if len(clears) > 0 {
		_ = json.Unmarshal(clears, &r.Clears)
	}
	return r, nil
}

func (j *pgJournal) query(ctx context.Context, where string, args ...any) ([]Redemption, error) {
	rows, err := j.db.QueryContext(ctx, `SELECT `+pgColumns+` FROM `+j.table+` `+where, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []Redemption
	for rows.Next() {
		r, errScan := scanRedemption(rows)
		if errScan != nil {
			return nil, errScan
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (j *pgJournal) pending(ctx context.Context, authID, creditID string, since time.Time) (*Redemption, error) {
	rows, err := j.query(ctx, `WHERE auth_id = $1 AND credit_id = $2 AND state = 'pending' AND created_at > $3
		ORDER BY id DESC LIMIT 1`, authID, creditID, since)
	if err != nil || len(rows) == 0 {
		return nil, err
	}
	return &rows[0], nil
}

func (j *pgJournal) unsettled(ctx context.Context, since time.Time) ([]Redemption, error) {
	return j.query(ctx, `WHERE state = 'pending' AND created_at > $1 ORDER BY id DESC LIMIT 100`, since)
}

func (j *pgJournal) list(ctx context.Context, q HistoryQuery) ([]Redemption, error) {
	var where []string
	var args []any
	add := func(cond string, v any) {
		args = append(args, v)
		where = append(where, strings.ReplaceAll(cond, "?", "$"+strconv.Itoa(len(args))))
	}
	if q.AuthID != "" {
		add("auth_id = ?", q.AuthID)
	}
	if q.Provider != "" {
		add("provider = ?", q.Provider)
	}
	if q.Outcome != "" {
		add("outcome = ?", q.Outcome)
	}
	if q.State != "" {
		add("state = ?", q.State)
	}
	if !q.From.IsZero() {
		add("created_at >= ?", q.From)
	}
	if !q.To.IsZero() {
		add("created_at < ?", q.To)
	}
	if q.BeforeID > 0 {
		add("id < ?", q.BeforeID)
	}
	clause := ""
	if len(where) > 0 {
		clause = "WHERE " + strings.Join(where, " AND ")
	}
	limit := q.Limit
	if limit <= 0 {
		limit = historyDefault
	}
	args = append(args, limit)
	return j.query(ctx, clause+` ORDER BY id DESC LIMIT $`+strconv.Itoa(len(args)), args...)
}

func jsonWindows(ws []Window) []byte {
	if ws == nil {
		ws = []Window{}
	}
	b, _ := json.Marshal(ws)
	return b
}

func (j *pgJournal) insert(ctx context.Context, r *Redemption) error {
	var clears []byte
	if r.Clears != nil {
		clears, _ = json.Marshal(r.Clears)
	}
	return j.db.QueryRowContext(ctx, `INSERT INTO `+j.table+` (created_at, auth_id, provider, label,
		credit_id, credit_title, request_id, state, outcome, before, after, actor, clears)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) RETURNING id`,
		r.CreatedAt, r.AuthID, r.Provider, r.Label, r.CreditID, r.CreditTitle, r.RequestID, r.State,
		r.Outcome, jsonWindows(r.Before), jsonWindows(r.After), r.Actor, clears).Scan(&r.ID)
}

func (j *pgJournal) settle(ctx context.Context, r *Redemption) error {
	_, err := j.db.ExecContext(ctx, `UPDATE `+j.table+` SET settled_at = $2, state = $3, outcome = $4,
		confirmed = $5, warning = $6, after = $7 WHERE id = $1`,
		r.ID, r.SettledAt, r.State, r.Outcome, r.Confirmed, r.Warning, jsonWindows(r.After))
	return err
}
