package pool

// Pool history (tokenmaxx fork): every TMX_POOL_SNAPSHOT_MINUTES (default 5) the same projection
// GET /tmx/pool returns is written to <schema>.pool_snapshots, one row per credential, all rows of
// one snapshot sharing snapshot_ts. The table is append-only and never pruned. Only what the pool
// endpoint shows is stored (masked auth_id and label; no token metadata).
//
// A snapshot whose insert fails stays queued (up to maxPending snapshots) and is retried on the
// next tick and at shutdown; anything dropped beyond that is counted and logged.

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/quota"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	log "github.com/sirupsen/logrus"
)

const (
	defaultSnapshotMins = 5
	maxPending          = 288 // one day of 5-minute snapshots
	historyDefaultLimit = 1000
	historyMaxLimit     = 10000
)

// BuildFunc returns the current pool projection (the /tmx/pool handler's data).
type BuildFunc func(context.Context) Snapshot

// HistoryRow is one credential in one stored snapshot.
type HistoryRow struct {
	ID             int64               `json:"id"`
	SnapshotTS     time.Time           `json:"snapshot_ts"`
	AuthID         string              `json:"auth_id"`
	AuthIndex      string              `json:"auth_index"`
	Provider       string              `json:"provider"`
	Label          string              `json:"label"`
	PlanType       string              `json:"plan_type"`
	Enabled        bool                `json:"enabled"`
	Status         string              `json:"status"`
	Unavailable    bool                `json:"unavailable"`
	Reason         string              `json:"reason"`
	NextRetryAfter *time.Time          `json:"next_retry_after"`
	InFlight       int                 `json:"in_flight"`
	BoundSessions  int                 `json:"bound_sessions"`
	PickRank       *int                `json:"pick_rank"`
	RankModel      string              `json:"rank_model"`
	CooldownCount  int                 `json:"cooldown_count"`
	Used5hPct      *float64            `json:"used_5h_pct"`
	Resets5hAt     *time.Time          `json:"resets_5h_at"`
	Used7dPct      *float64            `json:"used_7d_pct"`
	Resets7dAt     *time.Time          `json:"resets_7d_at"`
	Cooldowns      []auth.CooldownView `json:"cooldowns"`
	Windows        []quota.Current     `json:"windows"`
}

// HistoryQuery selects stored rows: snapshot_ts in [From, To), optional AuthID (the masked id
// /tmx/pool shows), rows with id > AfterID, oldest first.
type HistoryQuery struct {
	From, To time.Time
	AuthID   string
	AfterID  int64
	Limit    int
}

// RecorderStats counts what the recorder did since start.
type RecorderStats struct {
	IntervalSeconds int64      `json:"interval_seconds"`
	Snapshots       int64      `json:"snapshots"`
	Rows            int64      `json:"rows"`
	WriteFailures   int64      `json:"write_failures"`
	Dropped         int64      `json:"dropped"`
	Pending         int        `json:"pending"`
	LastSnapshotAt  *time.Time `json:"last_snapshot_at"`
	LastError       string     `json:"last_error,omitempty"`
}

// Recorder takes and stores pool snapshots.
type Recorder struct {
	db    *sql.DB
	table string
	build BuildFunc
	every time.Duration

	snapshots, rows, failures, dropped atomic.Int64

	mu       sync.Mutex // guards pending, lastAt, lastErr; held across a flush
	pending  []Snapshot
	lastAt   time.Time
	lastErr  string
	started  bool
	stop     chan struct{}
	done     chan struct{}
	stopOnce sync.Once
}

var (
	recorderMu sync.Mutex
	recorder   *Recorder
)

// DefaultRecorder returns the running recorder, or nil.
func DefaultRecorder() *Recorder {
	recorderMu.Lock()
	defer recorderMu.Unlock()
	return recorder
}

// SetDefaultRecorderForTest replaces the running recorder (nil clears it).
func SetDefaultRecorderForTest(r *Recorder) {
	recorderMu.Lock()
	defer recorderMu.Unlock()
	recorder = r
}

// SnapshotEvery reads TMX_POOL_SNAPSHOT_MINUTES (default 5); 0 or "off" disables the recorder.
func SnapshotEvery() time.Duration {
	raw := strings.TrimSpace(os.Getenv("TMX_POOL_SNAPSHOT_MINUTES"))
	if strings.EqualFold(raw, "off") {
		return 0
	}
	if n, err := strconv.Atoi(raw); err == nil && n >= 0 {
		return time.Duration(n) * time.Minute
	}
	return defaultSnapshotMins * time.Minute
}

// StartRecorder creates the table and the snapshot loop once. db/schema are the request-log
// database ("tmx").
func StartRecorder(db *sql.DB, schema string, build BuildFunc) *Recorder {
	recorderMu.Lock()
	defer recorderMu.Unlock()
	if recorder != nil || db == nil || build == nil {
		return recorder
	}
	every := SnapshotEvery()
	if every <= 0 {
		log.Info("tmx pool history: disabled (TMX_POOL_SNAPSHOT_MINUTES=0)")
		return nil
	}
	r, err := NewRecorder(context.Background(), db, schema, build, every)
	if err != nil {
		log.WithError(err).Warn("tmx pool history: not started")
		return nil
	}
	recorder = r
	r.started = true
	go r.loop()
	log.Infof("tmx pool history: recording to %s every %s", r.table, every)
	return r
}

// NewRecorder ensures the table and returns a recorder without starting its loop (tests call
// Tick directly).
func NewRecorder(ctx context.Context, db *sql.DB, schema string, build BuildFunc, every time.Duration) (*Recorder, error) {
	r := &Recorder{db: db, table: schema + ".pool_snapshots", build: build, every: every,
		stop: make(chan struct{}), done: make(chan struct{})}
	for _, stmt := range []string{
		`CREATE TABLE IF NOT EXISTS ` + r.table + ` (
			id               BIGSERIAL PRIMARY KEY,
			snapshot_ts      TIMESTAMPTZ NOT NULL,
			auth_id          TEXT NOT NULL,
			auth_index       TEXT NOT NULL DEFAULT '',
			provider         TEXT NOT NULL,
			label            TEXT NOT NULL DEFAULT '',
			plan_type        TEXT NOT NULL DEFAULT '',
			enabled          BOOLEAN NOT NULL,
			status           TEXT NOT NULL DEFAULT '',
			unavailable      BOOLEAN NOT NULL,
			reason           TEXT NOT NULL DEFAULT '',
			next_retry_after TIMESTAMPTZ,
			in_flight        INTEGER NOT NULL,
			bound_sessions   INTEGER NOT NULL,
			pick_rank        INTEGER,
			rank_model       TEXT NOT NULL DEFAULT '',
			cooldown_count   INTEGER NOT NULL,
			used_5h_pct      DOUBLE PRECISION,
			resets_5h_at     TIMESTAMPTZ,
			used_7d_pct      DOUBLE PRECISION,
			resets_7d_at     TIMESTAMPTZ,
			cooldowns        JSONB NOT NULL,
			windows          JSONB NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS pool_snapshots_ts ON ` + r.table + ` (snapshot_ts)`,
		`CREATE INDEX IF NOT EXISTS pool_snapshots_auth_ts ON ` + r.table + ` (auth_id, snapshot_ts)`,
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return nil, fmt.Errorf("pool history schema: %w", err)
		}
	}
	return r, nil
}

func (r *Recorder) loop() {
	defer close(r.done)
	ticker := time.NewTicker(r.every)
	defer ticker.Stop()
	for {
		select {
		case <-r.stop:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			_ = r.Tick(ctx)
			cancel()
		}
	}
}

// Tick takes one snapshot, queues it and writes everything queued, oldest first.
func (r *Recorder) Tick(ctx context.Context) error {
	snap := r.build(ctx)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pending = append(r.pending, snap)
	if over := len(r.pending) - maxPending; over > 0 {
		r.pending = r.pending[over:]
		r.dropped.Add(int64(over))
		log.Warnf("tmx pool history: dropped %d unwritten snapshots (database unavailable)", over)
	}
	return r.flushLocked(ctx)
}

func (r *Recorder) flushLocked(ctx context.Context) error {
	for len(r.pending) > 0 {
		snap := r.pending[0]
		if err := r.insert(ctx, snap); err != nil {
			r.failures.Add(1)
			r.lastErr = err.Error()
			log.WithError(err).Warnf("tmx pool history: write failed; %d snapshots queued for retry", len(r.pending))
			return err
		}
		r.pending = r.pending[1:]
		r.snapshots.Add(1)
		r.rows.Add(int64(len(snap.Credentials)))
		r.lastAt, r.lastErr = snap.ObservedAt, ""
	}
	return nil
}

// insert writes one snapshot in a transaction (all of its rows, or none).
func (r *Recorder) insert(ctx context.Context, snap Snapshot) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	ts := snap.ObservedAt.UTC().Truncate(time.Microsecond)
	for _, c := range snap.Credentials {
		cooldowns := c.Cooldowns
		if cooldowns == nil {
			cooldowns = []auth.CooldownView{}
		}
		windows := c.Windows
		if windows == nil {
			windows = []quota.Current{}
		}
		cj, _ := json.Marshal(cooldowns)
		wj, _ := json.Marshal(windows)
		var u5, u7 *float64
		var r5, r7 *time.Time
		for _, w := range windows {
			switch w.Window.Window {
			case "5h":
				u5, r5 = w.UsedPct, w.ResetsAt
			case "7d":
				u7, r7 = w.UsedPct, w.ResetsAt
			}
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO `+r.table+` (snapshot_ts, auth_id, auth_index, provider,
			label, plan_type, enabled, status, unavailable, reason, next_retry_after, in_flight, bound_sessions,
			pick_rank, rank_model, cooldown_count, used_5h_pct, resets_5h_at, used_7d_pct, resets_7d_at,
			cooldowns, windows)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22)`,
			ts, c.AuthID, c.AuthIndex, c.Provider, c.Label, c.PlanType, c.Enabled, string(c.Status),
			c.Unavailable, c.Reason, c.NextRetryAfter, c.InFlight, c.BoundSessions, c.PickRank, c.RankModel,
			len(cooldowns), u5, r5, u7, r7, string(cj), string(wj)); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// CloseContext stops the loop and retries queued snapshots once, bounded by ctx.
func (r *Recorder) CloseContext(ctx context.Context) error {
	if r == nil {
		return nil
	}
	r.stopOnce.Do(func() { close(r.stop) })
	if r.started {
		select {
		case <-r.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.flushLocked(ctx)
}

// Stats reports counters since start.
func (r *Recorder) Stats() RecorderStats {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := RecorderStats{IntervalSeconds: int64(r.every / time.Second), Snapshots: r.snapshots.Load(),
		Rows: r.rows.Load(), WriteFailures: r.failures.Load(), Dropped: r.dropped.Load(),
		Pending: len(r.pending), LastError: r.lastErr}
	if !r.lastAt.IsZero() {
		t := r.lastAt
		s.LastSnapshotAt = &t
	}
	return s
}

// History returns stored rows oldest first, and the id to pass as AfterID for the next page
// (0 when this was the last).
func (r *Recorder) History(ctx context.Context, q HistoryQuery) ([]HistoryRow, int64, error) {
	if q.Limit <= 0 || q.Limit > historyMaxLimit {
		q.Limit = historyDefaultLimit
	}
	args := []any{q.From, q.To, q.AfterID}
	where := `snapshot_ts >= $1 AND snapshot_ts < $2 AND id > $3`
	if q.AuthID != "" {
		args = append(args, q.AuthID)
		where += fmt.Sprintf(" AND auth_id = $%d", len(args))
	}
	args = append(args, q.Limit+1)
	rows, err := r.db.QueryContext(ctx, `SELECT id, snapshot_ts, auth_id, auth_index, provider, label,
		plan_type, enabled, status, unavailable, reason, next_retry_after, in_flight, bound_sessions,
		pick_rank, rank_model, cooldown_count, used_5h_pct, resets_5h_at, used_7d_pct, resets_7d_at,
		cooldowns, windows FROM `+r.table+` WHERE `+where+fmt.Sprintf(" ORDER BY id LIMIT $%d", len(args)), args...)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = rows.Close() }()
	out := []HistoryRow{}
	for rows.Next() {
		var h HistoryRow
		var retry, r5, r7 sql.NullTime
		var rank sql.NullInt64
		var u5, u7 sql.NullFloat64
		var cj, wj []byte
		if err = rows.Scan(&h.ID, &h.SnapshotTS, &h.AuthID, &h.AuthIndex, &h.Provider, &h.Label, &h.PlanType,
			&h.Enabled, &h.Status, &h.Unavailable, &h.Reason, &retry, &h.InFlight, &h.BoundSessions, &rank,
			&h.RankModel, &h.CooldownCount, &u5, &r5, &u7, &r7, &cj, &wj); err != nil {
			return nil, 0, err
		}
		h.SnapshotTS = h.SnapshotTS.UTC()
		h.NextRetryAfter = utcPtr(retry)
		h.Resets5hAt, h.Resets7dAt = utcPtr(r5), utcPtr(r7)
		if rank.Valid {
			v := int(rank.Int64)
			h.PickRank = &v
		}
		if u5.Valid {
			h.Used5hPct = &u5.Float64
		}
		if u7.Valid {
			h.Used7dPct = &u7.Float64
		}
		h.Cooldowns, h.Windows = []auth.CooldownView{}, []quota.Current{}
		_ = json.Unmarshal(cj, &h.Cooldowns)
		_ = json.Unmarshal(wj, &h.Windows)
		out = append(out, h)
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

func utcPtr(t sql.NullTime) *time.Time {
	if !t.Valid {
		return nil
	}
	v := t.Time.UTC()
	return &v
}
