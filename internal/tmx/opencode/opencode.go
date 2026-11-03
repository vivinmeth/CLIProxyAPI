// Package opencode tracks OpenCode Go allowance (tokenmaxx fork).
//
// What OpenCode Go does (docs plus a deliberate limit hit on staging, 2026-11-03): a plan has one
// monthly allowance pool in dollars (Go: $60), with a 5-hour window at 20% of it, a weekly one at
// 50% and the month at 100%. Each response reports what the request used as
// usage.opencode.allowance, already weighted by the model's "monthly limit" from the docs table:
// allowance = list_cost x pool / model_limit (a $15-limit model counts 4x its list price on Go, a
// $60-limit model 1x). When the pool's window is spent, every Go model answers 429
// GoUsageLimitError with Retry-After until the window ends. The 5-hour window starts at the first
// request after the previous one ended (Retry-After pointed 5 h after the first request).
//
// Readings here: the sum of allowance_usd (request log) over each window, divided by the window's
// share of the pool. The 5-hour window is anchored at first use, so it has a reset time; the weekly
// and monthly anchors are unknown, so those are rolling sums (an upper bound) without one. Readings
// are for the dashboard: they never cool a model down. Cooldown comes from OpenCode's own 429.
//
// Settings (plan, windows, per-plan pool and per-model limits) are configuration, kept in
// <schema>.opencode_quota rather than in the proxy config so an older image never meets an unknown
// config field. A new plan is a new entry under plans, no code change. A plan without a pool takes
// it from traffic (model_limit x allowance / list_cost, median over models with a known limit);
// with neither, readings say unknown_cap.
package opencode

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

// Window is one allowance window: Name (as shown in quota readings), Hours long, Share of the
// monthly pool. Anchored windows start at the first request after the previous one ended; others are
// rolling sums ending now.
type Window struct {
	Name     string  `json:"name"`
	Hours    float64 `json:"hours"`
	Share    float64 `json:"share"`
	Anchored bool    `json:"anchored,omitempty"`
}

// DefaultWindows are OpenCode Go's windows: 5-hour 20% (anchored at first use, observed), weekly 50%
// and monthly 100% (anchors unknown: rolling; the month read as 31 days so it never under-reads).
var DefaultWindows = []Window{{"5h", 5, 0.2, true}, {"7d", 168, 0.5, false}, {"30d", 744, 1, false}}

// Plan is one plan's allowance: the monthly pool (0 = take it from traffic) and each model's
// monthly limit from OpenCode's table (what weights its allowance; used to derive the pool).
type Plan struct {
	Pool float64            `json:"pool,omitempty"`
	Caps map[string]float64 `json:"caps"`
}

// Settings configure one OpenCode Go subscription.
type Settings struct {
	// Entry is the openai-compatibility entry name serving it, lowercased. Its request-log rows carry
	// provider "openai-compatible-<entry>" (the synthesized credential's provider).
	Entry string `json:"entry"`
	// Plan picks one of Plans ("go", "go-plus", ...).
	Plan string `json:"plan"`
	// Windows default to DefaultWindows when empty.
	Windows []Window        `json:"windows,omitempty"`
	Plans   map[string]Plan `json:"plans"`
}

// Normalize lowercases names, fills default windows and validates. It returns an error for a
// setting the tracker can't use.
func (s *Settings) Normalize() error {
	s.Entry = strings.ToLower(strings.TrimSpace(s.Entry))
	s.Plan = strings.ToLower(strings.TrimSpace(s.Plan))
	if s.Entry == "" {
		return errors.New("entry is required")
	}
	if s.Plan == "" {
		return errors.New("plan is required")
	}
	if len(s.Windows) == 0 {
		s.Windows = append([]Window(nil), DefaultWindows...)
	}
	seen := map[string]bool{}
	for i := range s.Windows {
		w := &s.Windows[i]
		w.Name = strings.TrimSpace(w.Name)
		if w.Name == "" || seen[w.Name] {
			return fmt.Errorf("window %d: name empty or repeated", i)
		}
		seen[w.Name] = true
		if w.Hours <= 0 || w.Hours > 24*400 || w.Share <= 0 || w.Share > 1 {
			return fmt.Errorf("window %s: hours must be in (0, 9600], share in (0, 1]", w.Name)
		}
	}
	plans := make(map[string]Plan, len(s.Plans))
	for name, p := range s.Plans {
		name = strings.ToLower(strings.TrimSpace(name))
		if name == "" {
			return errors.New("plan name empty")
		}
		if !finitePositive(p.Pool) && p.Pool != 0 {
			return fmt.Errorf("plan %s: pool must be > 0, or 0 to take it from traffic", name)
		}
		caps := make(map[string]float64, len(p.Caps))
		for model, v := range p.Caps {
			if !finitePositive(v) {
				return fmt.Errorf("plan %s model %s: limit must be > 0 (leave a model out when it is unknown)", name, model)
			}
			caps[strings.TrimSpace(model)] = v
		}
		plans[name] = Plan{Pool: p.Pool, Caps: caps}
	}
	s.Plans = plans
	return nil
}

func finitePositive(v float64) bool { return v > 0 && !math.IsNaN(v) && !math.IsInf(v, 0) }

func (s Settings) longest() time.Duration {
	h := 0.0
	for _, w := range s.Windows {
		h = math.Max(h, w.Hours)
	}
	return hours(h)
}

func hours(h float64) time.Duration { return time.Duration(h * float64(time.Hour)) }

// Store keeps settings in <schema>.opencode_quota and reads allowance from <schema>.requests.
type Store struct {
	db     *sql.DB
	schema string
}

// NewStore creates the settings table. db/schema are the request log's.
func NewStore(ctx context.Context, db *sql.DB, schema string) (*Store, error) {
	if db == nil {
		return nil, errors.New("no database")
	}
	s := &Store{db: db, schema: schema}
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS `+s.table()+` (
		entry      TEXT PRIMARY KEY,
		settings   JSONB NOT NULL,
		updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
	)`); err != nil {
		return nil, fmt.Errorf("opencode quota schema: %w", err)
	}
	return s, nil
}

func (s *Store) table() string { return s.schema + ".opencode_quota" }

// List returns every stored subscription, by entry.
func (s *Store) List(ctx context.Context) ([]Settings, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT settings FROM `+s.table()+` ORDER BY entry`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []Settings
	for rows.Next() {
		var raw []byte
		if err = rows.Scan(&raw); err != nil {
			return nil, err
		}
		var st Settings
		if err = json.Unmarshal(raw, &st); err != nil || st.Normalize() != nil {
			continue // stored in an older shape or before a stricter rule: skipped, not fatal
		}
		out = append(out, st)
	}
	return out, rows.Err()
}

// Put stores (replaces) one subscription's settings.
func (s *Store) Put(ctx context.Context, st Settings) (Settings, error) {
	if err := st.Normalize(); err != nil {
		return st, err
	}
	raw, err := json.Marshal(st)
	if err != nil {
		return st, err
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO `+s.table()+` (entry, settings, updated_at) VALUES ($1, $2, now())
		ON CONFLICT (entry) DO UPDATE SET settings = EXCLUDED.settings, updated_at = now()`, st.Entry, raw)
	return st, err
}

// Delete removes one subscription's settings; false when there were none.
func (s *Store) Delete(ctx context.Context, entry string) (bool, error) {
	res, err := s.db.ExecContext(ctx, `DELETE FROM `+s.table()+` WHERE entry = $1`, strings.ToLower(strings.TrimSpace(entry)))
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// Status values of a window reading.
const (
	StatusAllowed    = "allowed"
	StatusWarning    = "allowed_warning" // >= 80% of the window
	StatusAtLimit    = "rejected"        // >= 100%: OpenCode is likely refusing every Go model
	StatusUnknownCap = "unknown_cap"     // no pool for the plan, configured or derivable
	warnAt           = 80.0
)

// Pool sources.
const (
	PoolFromSettings = "settings"
	PoolFromTraffic  = "traffic"
)

// WindowReading is one window of a subscription.
type WindowReading struct {
	Window   string     `json:"window"`
	UsedUSD  float64    `json:"used_usd"`
	CapUSD   *float64   `json:"cap_usd"`   // nil when the pool is unknown
	UsedPct  *float64   `json:"used_pct"`  // nil when the pool is unknown
	ResetsAt *time.Time `json:"resets_at"` // anchored windows with usage only
	Status   string     `json:"status"`
}

// ModelUse is one model's allowance and list cost over the longest window.
type ModelUse struct {
	Model        string  `json:"model"`
	AllowanceUSD float64 `json:"allowance_usd"`
	ListCostUSD  float64 `json:"list_cost_usd"`
	Requests     int     `json:"requests"`
}

// Reading is one subscription's state.
type Reading struct {
	Entry      string          `json:"entry"`
	Plan       string          `json:"plan"`
	PoolUSD    *float64        `json:"pool_usd"`
	PoolSource string          `json:"pool_source,omitempty"` // settings | traffic | "" (unknown)
	Windows    []WindowReading `json:"windows"`
	Models     []ModelUse      `json:"models"`
}

// rowFilter selects st's rows that carry allowance: $1 is the entry.
const rowFilter = `provider IN ($1, 'openai-compatible-' || $1) AND allowance_usd IS NOT NULL AND NOT shadowed`

// Read computes st's reading at now.
func (s *Store) Read(ctx context.Context, st Settings, now time.Time) (Reading, error) {
	r := Reading{Entry: st.Entry, Plan: st.Plan}
	models, err := s.modelUse(ctx, st, now)
	if err != nil {
		return r, err
	}
	r.Models = models
	if pool, src := poolFor(st, models); src != "" {
		r.PoolUSD, r.PoolSource = &pool, src
	}
	for _, w := range st.Windows {
		var wr WindowReading
		if w.Anchored {
			wr, err = s.anchored(ctx, st.Entry, w, now)
		} else {
			wr, err = s.rolling(ctx, st.Entry, w, now)
		}
		if err != nil {
			return r, err
		}
		applyCap(&wr, r.PoolUSD, w.Share)
		r.Windows = append(r.Windows, wr)
	}
	return r, nil
}

func applyCap(wr *WindowReading, pool *float64, share float64) {
	wr.Status = StatusUnknownCap
	if pool == nil {
		return
	}
	capUSD := *pool * share
	pct := 100 * wr.UsedUSD / capUSD
	wr.CapUSD, wr.UsedPct = &capUSD, &pct
	switch {
	case pct >= 100:
		wr.Status = StatusAtLimit
	case pct >= warnAt:
		wr.Status = StatusWarning
	default:
		wr.Status = StatusAllowed
	}
}

// poolFor is the plan's configured pool, else the median of model_limit x allowance / list_cost
// over the models with a known limit and list cost.
func poolFor(st Settings, models []ModelUse) (float64, string) {
	plan, ok := st.Plans[st.Plan]
	if !ok {
		return 0, ""
	}
	if plan.Pool > 0 {
		return plan.Pool, PoolFromSettings
	}
	var est []float64
	for _, m := range models {
		if limit, ok := plan.Caps[m.Model]; ok && m.ListCostUSD > 0 && m.AllowanceUSD > 0 {
			est = append(est, limit*m.AllowanceUSD/m.ListCostUSD)
		}
	}
	if len(est) == 0 {
		return 0, ""
	}
	sort.Float64s(est)
	mid := est[len(est)/2]
	if len(est)%2 == 0 {
		mid = (est[len(est)/2-1] + mid) / 2
	}
	return math.Round(mid*100) / 100, PoolFromTraffic
}

func (s *Store) modelUse(ctx context.Context, st Settings, now time.Time) ([]ModelUse, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT model, sum(allowance_usd),
		COALESCE(sum(cost_usd) FILTER (WHERE price_source = 'opencode'), 0), count(*)
		FROM `+s.schema+`.requests WHERE `+rowFilter+` AND ts > $2 AND ts <= $3 GROUP BY model ORDER BY model`,
		st.Entry, now.Add(-st.longest()), now)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := []ModelUse{}
	for rows.Next() {
		var m ModelUse
		if err = rows.Scan(&m.Model, &m.AllowanceUSD, &m.ListCostUSD, &m.Requests); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

func (s *Store) rolling(ctx context.Context, entry string, w Window, now time.Time) (WindowReading, error) {
	wr := WindowReading{Window: w.Name}
	err := s.db.QueryRowContext(ctx, `SELECT COALESCE(sum(allowance_usd), 0) FROM `+s.schema+`.requests
		WHERE `+rowFilter+` AND ts > $2 AND ts <= $3`, entry, now.Add(-hours(w.Hours)), now).Scan(&wr.UsedUSD)
	return wr, err
}

// anchored replays the window chain over the last four window lengths: a window opens at the first
// request after the previous one closed and lasts w.Hours. The open window, if any, is the reading.
func (s *Store) anchored(ctx context.Context, entry string, w Window, now time.Time) (WindowReading, error) {
	wr := WindowReading{Window: w.Name}
	length := hours(w.Hours)
	rows, err := s.db.QueryContext(ctx, `SELECT ts, allowance_usd FROM `+s.schema+`.requests
		WHERE `+rowFilter+` AND ts > $2 AND ts <= $3 ORDER BY ts`, entry, now.Add(-4*length), now)
	if err != nil {
		return wr, err
	}
	defer func() { _ = rows.Close() }()
	var start time.Time
	var sum float64
	for rows.Next() {
		var ts time.Time
		var v float64
		if err = rows.Scan(&ts, &v); err != nil {
			return wr, err
		}
		if start.IsZero() || !ts.Before(start.Add(length)) {
			start, sum = ts, 0
		}
		sum += v
	}
	if err = rows.Err(); err != nil {
		return wr, err
	}
	if !start.IsZero() && now.Before(start.Add(length)) {
		end := start.Add(length).UTC()
		wr.UsedUSD, wr.ResetsAt = sum, &end
	}
	return wr, nil
}

// AccountID names a subscription's quota account: stable across key rotation (the entry name, not
// the credential's id, which hashes the key).
func AccountID(entry string) string { return "opencode:" + entry }
