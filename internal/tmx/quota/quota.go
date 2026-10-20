// Package quota records every account's quota windows over time (tokenmaxx fork).
//
// Two sources feed it:
//   - passive: the rate-limit headers the proxy already keeps per account from real traffic
//     (Claude 5h / 7d), read from the auth manager every minute;
//   - polled: GET api.anthropic.com/api/oauth/usage every TMX_QUOTA_POLL_MINUTES (default 10) per
//     account, which also covers model-scoped weekly limits (e.g. Fable) and idle accounts, plus
//     /api/oauth/profile every 6 h for the plan label.
//
// A snapshot row is written when a window's reading changes, and at least every 30 minutes as a
// heartbeat, so history shows both burn rate and "nothing happened". Codex is out of scope here.
package quota

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	log "github.com/sirupsen/logrus"
)

const (
	tickEvery       = time.Minute
	heartbeatEvery  = 30 * time.Minute
	planEvery       = 6 * time.Hour
	defaultPollMins = 10
	rateLimitPause  = 30 * time.Minute
	authErrorPause  = time.Hour

	claudeUsageURL   = "https://api.anthropic.com/api/oauth/usage"
	claudeProfileURL = "https://api.anthropic.com/api/oauth/profile"
)

var claudeHeaders = map[string]string{
	"User-Agent":     "claude-cli/2.1.280 (external, cli)",
	"Authorization":  "Bearer $TOKEN$",
	"Content-Type":   "application/json",
	"Accept":         "application/json",
	"anthropic-beta": "oauth-2025-04-20",
}

// Fetcher is what the service needs from the management layer: the current accounts, and an
// authenticated GET that refreshes the account token and uses the account's proxy settings.
type Fetcher interface {
	TmxAuths() []*coreauth.Auth
	TmxFetch(ctx context.Context, auth *coreauth.Auth, url string, headers map[string]string) (int, []byte, error)
}

// Current is the latest reading of one window.
type Current struct {
	Window
	ObservedAt time.Time `json:"observed_at"`
	Source     string    `json:"source"` // passive | polled
}

// Account is one account's latest state.
type Account struct {
	AuthID     string     `json:"auth_id"`
	AuthIndex  string     `json:"auth_index"`
	Provider   string     `json:"provider"`
	Label      string     `json:"label"`
	Plan       string     `json:"plan"`
	Windows    []Current  `json:"windows"`
	LastPollAt *time.Time `json:"last_poll_at"`
	NextPollAt *time.Time `json:"next_poll_at"`
	PollError  string     `json:"poll_error,omitempty"`
}

type written struct {
	used    *float64
	resets  *time.Time
	status  string
	writtAt time.Time
}

type account struct {
	Account
	current     map[string]Current
	written     map[string]written
	lastPassive time.Time
	planAt      time.Time
	pauseUntil  time.Time
}

// Service collects and stores quota snapshots.
type Service struct {
	db        *sql.DB
	table     string
	fetch     Fetcher
	pollEvery time.Duration
	now       func() time.Time

	mu       sync.Mutex
	accounts map[string]*account
}

var (
	defaultMu sync.Mutex
	current   *Service
)

// Default returns the running service, or nil.
func Default() *Service {
	defaultMu.Lock()
	defer defaultMu.Unlock()
	return current
}

// Start creates the table and the collection loop once. db is the request-log database; schema
// its schema ("tmx"). TMX_QUOTA=off disables it.
func Start(db *sql.DB, schema string, fetch Fetcher) *Service {
	defaultMu.Lock()
	defer defaultMu.Unlock()
	if current != nil || db == nil || fetch == nil {
		return current
	}
	if strings.EqualFold(strings.TrimSpace(os.Getenv("TMX_QUOTA")), "off") {
		log.Info("tmx quota: disabled (TMX_QUOTA=off)")
		return nil
	}
	s, err := newService(context.Background(), db, schema, fetch)
	if err != nil {
		log.WithError(err).Warn("tmx quota: not started")
		return nil
	}
	current = s
	go s.loop()
	log.Infof("tmx quota: recording to %s (poll every %s)", s.table, s.pollEvery)
	return s
}

func newService(ctx context.Context, db *sql.DB, schema string, fetch Fetcher) (*Service, error) {
	poll := defaultPollMins
	if n, err := strconv.Atoi(strings.TrimSpace(os.Getenv("TMX_QUOTA_POLL_MINUTES"))); err == nil && n >= 2 {
		poll = n
	}
	s := &Service{db: db, table: schema + ".quota_snapshots", fetch: fetch,
		pollEvery: time.Duration(poll) * time.Minute, now: time.Now, accounts: map[string]*account{}}
	for _, stmt := range []string{
		`CREATE TABLE IF NOT EXISTS ` + s.table + ` (
			id         BIGSERIAL PRIMARY KEY,
			ts         TIMESTAMPTZ NOT NULL,
			auth_id    TEXT NOT NULL,
			provider   TEXT NOT NULL,
			label      TEXT NOT NULL DEFAULT '',
			plan       TEXT NOT NULL DEFAULT '',
			"window"   TEXT NOT NULL,
			used_pct   DOUBLE PRECISION,
			resets_at  TIMESTAMPTZ,
			status     TEXT NOT NULL DEFAULT '',
			source     TEXT NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS quota_snapshots_auth_window_ts ON ` + s.table + ` (auth_id, "window", ts)`,
		`CREATE INDEX IF NOT EXISTS quota_snapshots_ts ON ` + s.table + ` (ts)`,
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return nil, fmt.Errorf("quota schema: %w", err)
		}
	}
	if err := s.loadLatest(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

// loadLatest restores the last written reading per window so a restart neither duplicates rows
// nor forgets the plan.
func (s *Service) loadLatest(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, `SELECT DISTINCT ON (auth_id, "window") auth_id, provider, label,
		plan, "window", used_pct, resets_at, status, source, ts FROM `+s.table+`
		ORDER BY auth_id, "window", ts DESC`)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var authID, provider, label, plan, window, status, source string
		var used sql.NullFloat64
		var resets sql.NullTime
		var ts time.Time
		if err = rows.Scan(&authID, &provider, &label, &plan, &window, &used, &resets, &status, &source, &ts); err != nil {
			return err
		}
		a := s.accountLocked(authID)
		a.Provider, a.Label = provider, label
		if plan != "" {
			a.Plan = plan
		}
		w := Window{Window: window, Status: status}
		if used.Valid {
			v := used.Float64
			w.UsedPct = &v
		}
		if resets.Valid {
			t := resets.Time.UTC()
			w.ResetsAt = &t
		}
		a.current[window] = Current{Window: w, ObservedAt: ts, Source: source}
		a.written[window] = written{used: w.UsedPct, resets: w.ResetsAt, status: status, writtAt: ts}
	}
	return rows.Err()
}

func (s *Service) accountLocked(authID string) *account {
	a := s.accounts[authID]
	if a == nil {
		a = &account{Account: Account{AuthID: authID}, current: map[string]Current{}, written: map[string]written{}}
		s.accounts[authID] = a
	}
	return a
}

func (s *Service) loop() {
	ticker := time.NewTicker(tickEvery)
	defer ticker.Stop()
	for {
		s.tick(context.Background())
		<-ticker.C
	}
}

// tick reads passive signals for every account and polls the ones that are due.
func (s *Service) tick(ctx context.Context) {
	now := s.now()
	for _, auth := range s.fetch.TmxAuths() {
		if auth == nil || auth.Disabled || !strings.EqualFold(auth.Provider, "claude") {
			continue
		}
		s.mu.Lock()
		a := s.accountLocked(auth.ID)
		a.AuthIndex, a.Provider = auth.EnsureIndex(), "claude"
		if label := strings.TrimSpace(auth.Label); label != "" {
			a.Label = label
		}
		observed := auth.Quota.ObservedAt
		passiveDue := !observed.IsZero() && observed.After(a.lastPassive)
		pollDue := (a.LastPollAt == nil || now.Sub(*a.LastPollAt) >= s.pollEvery) && !now.Before(a.pauseUntil)
		planDue := now.Sub(a.planAt) >= planEvery
		s.mu.Unlock()

		if passiveDue {
			s.record(ctx, auth.ID, ParseClaudeSignals(auth.Quota.Signals), "passive", observed.UTC())
			s.mu.Lock()
			a.lastPassive = observed
			s.mu.Unlock()
		}
		if pollDue {
			s.poll(ctx, auth, a, planDue)
		}
	}
}

func (s *Service) poll(ctx context.Context, auth *coreauth.Auth, a *account, planDue bool) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	now := s.now().UTC()
	status, body, err := s.fetch.TmxFetch(ctx, auth, claudeUsageURL, cloneHeaders())
	var windows []Window
	pollErr := ""
	switch {
	case err != nil:
		pollErr = err.Error()
	case status == 429:
		pollErr = "rate limited (429)"
	case status == 401 || status == 403:
		pollErr = fmt.Sprintf("unauthorized (%d)", status)
	case status < 200 || status >= 300:
		pollErr = fmt.Sprintf("status %d", status)
	default:
		if windows, err = ParseClaudeUsage(body); err != nil {
			pollErr = "bad usage payload: " + err.Error()
		}
	}
	if planDue && pollErr == "" {
		if st, pb, errPlan := s.fetch.TmxFetch(ctx, auth, claudeProfileURL, cloneHeaders()); errPlan == nil && st == 200 {
			if plan := ParseClaudePlan(pb); plan != "" {
				s.mu.Lock()
				a.Plan, a.planAt = plan, now
				s.mu.Unlock()
			}
		}
	}
	s.mu.Lock()
	a.LastPollAt, a.PollError = &now, pollErr
	switch {
	case status == 429:
		a.pauseUntil = now.Add(rateLimitPause)
	case status == 401 || status == 403:
		a.pauseUntil = now.Add(authErrorPause)
	}
	s.mu.Unlock()
	if pollErr != "" {
		log.WithField("auth_id", auth.ID).Debugf("tmx quota: poll failed: %s", pollErr)
		return
	}
	s.record(ctx, auth.ID, windows, "polled", now)
}

func cloneHeaders() map[string]string {
	out := make(map[string]string, len(claudeHeaders))
	for k, v := range claudeHeaders {
		out[k] = v
	}
	return out
}

// record updates current state and writes a snapshot for windows that changed or are due a
// heartbeat. Older readings never overwrite newer ones.
func (s *Service) record(ctx context.Context, authID string, windows []Window, source string, at time.Time) {
	type pending struct {
		w Window
	}
	var toWrite []pending
	s.mu.Lock()
	a := s.accountLocked(authID)
	label, provider, plan := a.Label, a.Provider, a.Plan
	for _, w := range windows {
		if cur, ok := a.current[w.Window]; ok && cur.ObservedAt.After(at) {
			continue
		}
		last, ok := a.written[w.Window]
		if w.Status == "" && ok {
			w.Status = last.status // the usage API has no status; keep the last one the headers gave
		}
		a.current[w.Window] = Current{Window: w, ObservedAt: at, Source: source}
		if ok && !changed(last, w) && at.Sub(last.writtAt) < heartbeatEvery {
			continue
		}
		a.written[w.Window] = written{used: w.UsedPct, resets: w.ResetsAt, status: w.Status, writtAt: at}
		toWrite = append(toWrite, pending{w: w})
	}
	s.mu.Unlock()
	for _, p := range toWrite {
		if _, err := s.db.ExecContext(ctx, `INSERT INTO `+s.table+` (ts, auth_id, provider, label, plan,
			"window", used_pct, resets_at, status, source) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
			at, authID, provider, label, plan, p.w.Window, p.w.UsedPct, p.w.ResetsAt, p.w.Status, source); err != nil {
			log.WithError(err).Warn("tmx quota: snapshot write failed")
		}
	}
}

func changed(last written, w Window) bool {
	if (last.used == nil) != (w.UsedPct == nil) ||
		(last.used != nil && math.Abs(*last.used-*w.UsedPct) >= 0.01) {
		return true
	}
	if (last.resets == nil) != (w.ResetsAt == nil) ||
		(last.resets != nil && math.Abs(last.resets.Sub(*w.ResetsAt).Seconds()) >= 60) {
		return true
	}
	return last.status != w.Status && w.Status != ""
}

// Accounts returns the latest state of every account, sorted by auth id.
func (s *Service) Accounts() []Account {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Account, 0, len(s.accounts))
	for _, a := range s.accounts {
		acc := a.Account
		acc.Windows = make([]Current, 0, len(a.current))
		for _, c := range a.current {
			acc.Windows = append(acc.Windows, c)
		}
		for i := 1; i < len(acc.Windows); i++ {
			for j := i; j > 0 && acc.Windows[j].Window.Window < acc.Windows[j-1].Window.Window; j-- {
				acc.Windows[j], acc.Windows[j-1] = acc.Windows[j-1], acc.Windows[j]
			}
		}
		if a.LastPollAt != nil {
			next := a.LastPollAt.Add(s.pollEvery)
			if a.pauseUntil.After(next) {
				next = a.pauseUntil
			}
			acc.NextPollAt = &next
		}
		out = append(out, acc)
	}
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j].AuthID < out[j-1].AuthID; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}

// Snapshot is one history row.
type Snapshot struct {
	TS       time.Time  `json:"ts"`
	AuthID   string     `json:"auth_id"`
	Provider string     `json:"provider"`
	Label    string     `json:"label"`
	Plan     string     `json:"plan"`
	Window   string     `json:"window"`
	UsedPct  *float64   `json:"used_pct"`
	ResetsAt *time.Time `json:"resets_at"`
	Status   string     `json:"status,omitempty"`
	Source   string     `json:"source"`
}

// History returns snapshots in [from, to), oldest first. Empty authID/window mean all.
func (s *Service) History(ctx context.Context, authID, window string, from, to time.Time, limit int) ([]Snapshot, bool, error) {
	q := `SELECT ts, auth_id, provider, label, plan, "window", used_pct, resets_at, status, source FROM ` +
		s.table + ` WHERE ts >= $1 AND ts < $2`
	args := []any{from, to}
	if authID != "" {
		args = append(args, authID)
		q += fmt.Sprintf(" AND auth_id = $%d", len(args))
	}
	if window != "" {
		args = append(args, window)
		q += fmt.Sprintf(` AND "window" = $%d`, len(args))
	}
	args = append(args, limit+1)
	q += fmt.Sprintf(" ORDER BY ts, id LIMIT $%d", len(args))
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = rows.Close() }()
	out := []Snapshot{}
	for rows.Next() {
		var r Snapshot
		var used sql.NullFloat64
		var resets sql.NullTime
		if err = rows.Scan(&r.TS, &r.AuthID, &r.Provider, &r.Label, &r.Plan, &r.Window, &used, &resets,
			&r.Status, &r.Source); err != nil {
			return nil, false, err
		}
		if used.Valid {
			v := used.Float64
			r.UsedPct = &v
		}
		if resets.Valid {
			t := resets.Time.UTC()
			r.ResetsAt = &t
		}
		out = append(out, r)
	}
	truncated := len(out) > limit
	if truncated {
		out = out[:limit]
	}
	return out, truncated, rows.Err()
}
