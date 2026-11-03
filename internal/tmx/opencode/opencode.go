// Package opencode tracks OpenCode Go allowance (tokenmaxx fork).
//
// OpenCode Go limits each model by dollars of a monthly cap, with a 5-hour window at 20% of it, a
// weekly one at 50% and the month at 100%. No API reports the remaining allowance; every response
// says how much the request used (usage.opencode.allowance, stored per request-log row as
// allowance_usd). Readings here are sums of those rows over rolling windows, divided by each
// window's share of the configured cap. OpenCode's window anchors are undocumented, so a rolling sum
// is an upper bound of the real usage: readings are for the dashboard only and never cool a model
// down. Cooldown comes from OpenCode's own limit response, like every other provider's.
//
// Settings (the plan and its per-model cap tables) are configuration, kept in <schema>.opencode_quota
// rather than in the proxy config so an older image never sees an unknown config field. A new plan
// is a new table there; a model without a cap for the configured plan reads "unknown_cap".
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
// monthly cap.
type Window struct {
	Name  string  `json:"name"`
	Hours float64 `json:"hours"`
	Share float64 `json:"share"`
}

// DefaultWindows are OpenCode Go's published windows (5-hour 20%, weekly 50%, monthly 100%). The
// month is read as 31 days, so the rolling sum never under-reads a calendar month.
var DefaultWindows = []Window{{"5h", 5, 0.2}, {"7d", 168, 0.5}, {"30d", 744, 1}}

// Settings configure one OpenCode Go subscription.
type Settings struct {
	// Entry is the openai-compatibility entry name serving it (its rows' provider, lowercased).
	Entry string `json:"entry"`
	// Plan picks the cap table in Plans ("go", "go-plus", ...).
	Plan string `json:"plan"`
	// Windows default to DefaultWindows when empty.
	Windows []Window `json:"windows,omitempty"`
	// Plans maps plan -> upstream model id -> monthly cap in USD.
	Plans map[string]map[string]float64 `json:"plans"`
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
	plans := make(map[string]map[string]float64, len(s.Plans))
	for plan, caps := range s.Plans {
		name := strings.ToLower(strings.TrimSpace(plan))
		if name == "" {
			return errors.New("plan name empty")
		}
		out := make(map[string]float64, len(caps))
		for model, capUSD := range caps {
			if math.IsNaN(capUSD) || math.IsInf(capUSD, 0) || capUSD <= 0 {
				return fmt.Errorf("plan %s model %s: cap must be > 0 (leave a model out when its cap is unknown)", name, model)
			}
			out[strings.TrimSpace(model)] = capUSD
		}
		plans[name] = out
	}
	s.Plans = plans
	return nil
}

// Cap returns the configured monthly cap for model under the current plan.
func (s Settings) Cap(model string) (float64, bool) {
	v, ok := s.Plans[s.Plan][model]
	return v, ok
}

// longest is the longest window, the range a reading has to scan.
func (s Settings) longest() time.Duration {
	h := 0.0
	for _, w := range s.Windows {
		h = math.Max(h, w.Hours)
	}
	return time.Duration(h * float64(time.Hour))
}

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
		if err = json.Unmarshal(raw, &st); err != nil {
			return nil, err
		}
		if err = st.Normalize(); err != nil {
			continue // stored before a stricter rule; skipped rather than failing every reading
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

// Status values of a reading.
const (
	StatusAllowed    = "allowed"
	StatusWarning    = "allowed_warning" // >= 80% of the window
	StatusAtLimit    = "rejected"        // >= 100%: OpenCode is likely refusing the model
	StatusUnknownCap = "unknown_cap"     // no cap for this model under the configured plan
	warnAt           = 80.0
)

// WindowReading is one window of one model.
type WindowReading struct {
	Window  string
	UsedUSD float64
	CapUSD  *float64 // nil when the cap is unknown
	UsedPct *float64 // nil when the cap is unknown
	Status  string
}

// ModelReading is one model's windows.
type ModelReading struct {
	Model   string
	Windows []WindowReading
}

// Readings sums allowance per model of st's entry over each window ending at now. Only models with
// usage inside the longest window are returned, sorted by model.
func (s *Store) Readings(ctx context.Context, st Settings, now time.Time) ([]ModelReading, error) {
	selects := make([]string, len(st.Windows))
	args := []any{st.Entry, now.Add(-st.longest()), now}
	for i, w := range st.Windows {
		args = append(args, now.Add(-time.Duration(w.Hours*float64(time.Hour))))
		selects[i] = fmt.Sprintf("COALESCE(sum(allowance_usd) FILTER (WHERE ts > $%d), 0)", len(args))
	}
	rows, err := s.db.QueryContext(ctx, `SELECT model, `+strings.Join(selects, ", ")+` FROM `+s.schema+`.requests
		WHERE lower(provider) = $1 AND allowance_usd IS NOT NULL AND ts > $2 AND ts <= $3 AND NOT shadowed
		GROUP BY model ORDER BY model`, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []ModelReading
	for rows.Next() {
		var model string
		sums := make([]float64, len(st.Windows))
		dest := []any{&model}
		for i := range sums {
			dest = append(dest, &sums[i])
		}
		if err = rows.Scan(dest...); err != nil {
			return nil, err
		}
		out = append(out, readingFor(st, model, sums))
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Model < out[j].Model })
	return out, nil
}

// readingFor turns one model's window sums into readings against its cap.
func readingFor(st Settings, model string, sums []float64) ModelReading {
	r := ModelReading{Model: model}
	monthly, known := st.Cap(model)
	for i, w := range st.Windows {
		wr := WindowReading{Window: w.Name, UsedUSD: sums[i], Status: StatusUnknownCap}
		if known {
			capUSD := monthly * w.Share
			pct := 100 * sums[i] / capUSD
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
		r.Windows = append(r.Windows, wr)
	}
	return r
}

// AccountID names a model's quota account: stable across key rotation (the entry name, not the
// credential's id, which hashes the key).
func AccountID(entry, model string) string { return "opencode:" + entry + ":" + model }
