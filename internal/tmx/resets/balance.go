package resets

// Balance history (tokenmaxx fork): every provider read of an account's reset credits is stored
// in <schema>.reset_credit_snapshots (append-only, never pruned): source "poll" for inventory
// reads (GET /tmx/resets and the background poll), "manual" for POST /tmx/resets/poll, "redeem"
// for the reads around a redeem. A failed read is stored too, with error set and the credit
// counts NULL (unknown, not zero).
//
// Rows whose insert fails stay queued (up to maxBalancePending) and are written with the next
// read; anything dropped beyond that is counted and logged.

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

// Sources of a balance snapshot.
const (
	SourcePoll   = "poll"
	SourceManual = "manual"
	SourceRedeem = "redeem"
)

const (
	maxBalancePending   = 5000
	balanceWriteTimeout = 10 * time.Second
	balanceDefaultLimit = 1000
	balanceMaxLimit     = 10000
)

// BalanceSnapshot is one read of one account's banked reset credits.
type BalanceSnapshot struct {
	ID        int64     `json:"id"`
	TS        time.Time `json:"ts"`
	AuthID    string    `json:"auth_id"`
	AuthIndex string    `json:"auth_index"`
	Provider  string    `json:"provider"`
	Source    string    `json:"source"` // poll | manual | redeem
	Error     string    `json:"error,omitempty"`
	// Credits: banked credits listed; ResetsLeft: their resets_left summed; Usable: credits with
	// no blocker. All nil when the read failed.
	Credits         *int       `json:"credits"`
	ResetsLeft      *int       `json:"resets_left"`
	Usable          *int       `json:"usable"`
	ApplicableCount *int       `json:"applicable_count"` // Codex only
	NextExpiresAt   *time.Time `json:"next_expires_at"`  // soonest credit expiry
	Eligible        bool       `json:"eligible"`
	AtLimit         bool       `json:"at_limit"`
	CooldownUntil   *time.Time `json:"cooldown_until"`
	CreditList      []Credit   `json:"credit_list"`
}

// BalanceQuery selects stored snapshots: ts in [From, To), optional filters, id > AfterID,
// oldest first.
type BalanceQuery struct {
	From, To time.Time
	AuthID   string
	Provider string
	Source   string
	AfterID  int64
	Limit    int
}

// BalanceStats counts balance writes since start.
type BalanceStats struct {
	Rows          int64  `json:"rows"`
	WriteFailures int64  `json:"write_failures"`
	Dropped       int64  `json:"dropped"`
	Pending       int    `json:"pending"`
	LastError     string `json:"last_error,omitempty"`
}

// balanceSnapshotOf turns a read into a snapshot row.
func balanceSnapshotOf(acct Account, source string, at time.Time) BalanceSnapshot {
	b := BalanceSnapshot{TS: at.UTC().Truncate(time.Microsecond), AuthID: acct.AuthID, AuthIndex: acct.AuthIndex,
		Provider: acct.Provider, Source: source, Error: acct.Error, Eligible: acct.Eligible, AtLimit: acct.AtLimit,
		CooldownUntil: acct.CooldownUntil, ApplicableCount: acct.ApplicableCount, CreditList: []Credit{}}
	if acct.CheckedAt != nil {
		b.TS = acct.CheckedAt.UTC().Truncate(time.Microsecond)
	}
	if acct.Error != "" {
		return b
	}
	credits, left, usable := len(acct.Credits), 0, 0
	for _, c := range acct.Credits {
		c.Verdict = nil
		b.CreditList = append(b.CreditList, c)
		left += c.ResetsLeft
		if c.Blocker == "" {
			usable++
		}
		if c.ExpiresAt != nil && (b.NextExpiresAt == nil || c.ExpiresAt.Before(*b.NextExpiresAt)) {
			t := c.ExpiresAt.UTC()
			b.NextExpiresAt = &t
		}
	}
	b.Credits, b.ResetsLeft, b.Usable = &credits, &left, &usable
	return b
}

type balanceLog struct {
	db    *sql.DB
	table string

	mu      sync.Mutex
	pending []BalanceSnapshot
	stats   BalanceStats
}

func newBalanceLog(ctx context.Context, db *sql.DB, schema string) (*balanceLog, error) {
	l := &balanceLog{db: db, table: schema + ".reset_credit_snapshots"}
	for _, stmt := range []string{
		`CREATE TABLE IF NOT EXISTS ` + l.table + ` (
			id               BIGSERIAL PRIMARY KEY,
			ts               TIMESTAMPTZ NOT NULL,
			auth_id          TEXT NOT NULL,
			auth_index       TEXT NOT NULL DEFAULT '',
			provider         TEXT NOT NULL,
			source           TEXT NOT NULL,
			error            TEXT NOT NULL DEFAULT '',
			credits          INTEGER,
			resets_left      INTEGER,
			usable           INTEGER,
			applicable_count INTEGER,
			next_expires_at  TIMESTAMPTZ,
			eligible         BOOLEAN NOT NULL,
			at_limit         BOOLEAN NOT NULL,
			cooldown_until   TIMESTAMPTZ,
			credit_list      JSONB NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS reset_credit_snapshots_ts ON ` + l.table + ` (ts)`,
		`CREATE INDEX IF NOT EXISTS reset_credit_snapshots_auth_ts ON ` + l.table + ` (auth_id, ts)`,
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return nil, fmt.Errorf("reset credit snapshots schema: %w", err)
		}
	}
	return l, nil
}

// record queues b and writes everything queued, oldest first.
func (l *balanceLog) record(ctx context.Context, b BalanceSnapshot) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.pending = append(l.pending, b)
	if over := len(l.pending) - maxBalancePending; over > 0 {
		l.pending = l.pending[over:]
		l.stats.Dropped += int64(over)
		log.Warnf("tmx resets: dropped %d unwritten balance snapshots (database unavailable)", over)
	}
	return l.flushLocked(ctx)
}

func (l *balanceLog) flushLocked(ctx context.Context) error {
	for len(l.pending) > 0 {
		if err := l.insert(ctx, l.pending[0]); err != nil {
			l.stats.WriteFailures++
			l.stats.LastError = err.Error()
			log.WithError(err).Warnf("tmx resets: balance snapshot write failed; %d queued for retry", len(l.pending))
			return err
		}
		l.pending = l.pending[1:]
		l.stats.Rows++
		l.stats.LastError = ""
	}
	return nil
}

func (l *balanceLog) insert(ctx context.Context, b BalanceSnapshot) error {
	list, _ := json.Marshal(b.CreditList)
	_, err := l.db.ExecContext(ctx, `INSERT INTO `+l.table+` (ts, auth_id, auth_index, provider, source, error,
		credits, resets_left, usable, applicable_count, next_expires_at, eligible, at_limit, cooldown_until, credit_list)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)`,
		b.TS, b.AuthID, b.AuthIndex, b.Provider, b.Source, b.Error, b.Credits, b.ResetsLeft, b.Usable,
		b.ApplicableCount, b.NextExpiresAt, b.Eligible, b.AtLimit, b.CooldownUntil, string(list))
	return err
}

func (l *balanceLog) snapshotStats() BalanceStats {
	l.mu.Lock()
	defer l.mu.Unlock()
	s := l.stats
	s.Pending = len(l.pending)
	return s
}

func (l *balanceLog) list(ctx context.Context, q BalanceQuery) ([]BalanceSnapshot, int64, error) {
	if q.Limit <= 0 || q.Limit > balanceMaxLimit {
		q.Limit = balanceDefaultLimit
	}
	args := []any{q.From, q.To, q.AfterID}
	where := []string{"ts >= $1", "ts < $2", "id > $3"}
	for col, v := range map[string]string{"auth_id": q.AuthID, "provider": q.Provider, "source": q.Source} {
		if v != "" {
			args = append(args, v)
			where = append(where, col+" = $"+strconv.Itoa(len(args)))
		}
	}
	args = append(args, q.Limit+1)
	rows, err := l.db.QueryContext(ctx, `SELECT id, ts, auth_id, auth_index, provider, source, error, credits,
		resets_left, usable, applicable_count, next_expires_at, eligible, at_limit, cooldown_until, credit_list
		FROM `+l.table+` WHERE `+strings.Join(where, " AND ")+` ORDER BY id LIMIT $`+strconv.Itoa(len(args)), args...)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = rows.Close() }()
	out := []BalanceSnapshot{}
	for rows.Next() {
		var b BalanceSnapshot
		var credits, left, usable, applicable sql.NullInt64
		var expires, cooldown sql.NullTime
		var list []byte
		if err = rows.Scan(&b.ID, &b.TS, &b.AuthID, &b.AuthIndex, &b.Provider, &b.Source, &b.Error, &credits,
			&left, &usable, &applicable, &expires, &b.Eligible, &b.AtLimit, &cooldown, &list); err != nil {
			return nil, 0, err
		}
		b.TS = b.TS.UTC()
		b.Credits, b.ResetsLeft, b.Usable, b.ApplicableCount = intPtr(credits), intPtr(left), intPtr(usable), intPtr(applicable)
		b.NextExpiresAt, b.CooldownUntil = timePtr(expires), timePtr(cooldown)
		b.CreditList = []Credit{}
		_ = json.Unmarshal(list, &b.CreditList)
		out = append(out, b)
	}
	if err = rows.Err(); err != nil {
		return nil, 0, err
	}
	var next int64
	if len(out) > q.Limit {
		out = out[:q.Limit]
		next = out[len(out)-1].ID
	}
	return out, next, nil
}

func intPtr(v sql.NullInt64) *int {
	if !v.Valid {
		return nil
	}
	n := int(v.Int64)
	return &n
}

func timePtr(v sql.NullTime) *time.Time {
	if !v.Valid {
		return nil
	}
	t := v.Time.UTC()
	return &t
}

// ErrNoBalanceHistory: balance history needs the request-log database.
var ErrNoBalanceHistory = fmt.Errorf("reset credit balance history needs the request-log database")

// BalanceHistory returns stored balance snapshots oldest first and the next page's AfterID
// (0 on the last page).
func (s *Service) BalanceHistory(ctx context.Context, q BalanceQuery) ([]BalanceSnapshot, int64, error) {
	if s.balances == nil {
		return nil, 0, ErrNoBalanceHistory
	}
	return s.balances.list(ctx, q)
}

// BalanceStats reports balance writes since start (zero without a database).
func (s *Service) BalanceStats() BalanceStats {
	if s.balances == nil {
		return BalanceStats{}
	}
	return s.balances.snapshotStats()
}

// recordBalance stores one read; detached from the caller so a cancelled request still records it.
func (s *Service) recordBalance(acct Account, source string) {
	if s.balances == nil || acct.AuthID == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), balanceWriteTimeout)
	defer cancel()
	_ = s.balances.record(ctx, balanceSnapshotOf(acct, source, s.now()))
}
