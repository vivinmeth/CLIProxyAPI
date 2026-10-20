package requestlog

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// Filter narrows every query. Empty fields don't filter.
//
// Sources: exact values ("tmx-proxy", "extern:claude-code") or "extern:*" for every imported
// source; several may be given. Shadowed rows (imported traffic the proxy already counted) are
// excluded unless IncludeShadowed is set.
type Filter struct {
	From            time.Time
	To              time.Time
	Sources         []string
	IncludeShadowed bool
	Outcome         string
	Provider        string
	AuthID          string
	Model           string
	SessionID       string
	APIKeyHash      string
	Machine         string
	Harness         string
	ClientMode      string
}

// where renders the WHERE clause; args start at $1.
func (f Filter) where() (string, []any) {
	conds := []string{"ts >= $1", "ts < $2"}
	args := []any{f.From, f.To}
	add := func(cond string, v any) {
		args = append(args, v)
		conds = append(conds, fmt.Sprintf(cond, len(args)))
	}
	if !f.IncludeShadowed {
		conds = append(conds, "NOT shadowed")
	}
	if len(f.Sources) > 0 {
		var parts []string
		for _, src := range f.Sources {
			src = strings.TrimSpace(src)
			switch {
			case src == "":
			case src == "extern:*" || src == "extern":
				parts = append(parts, "source LIKE 'extern:%'")
			default:
				args = append(args, src)
				parts = append(parts, fmt.Sprintf("source = $%d", len(args)))
			}
		}
		if len(parts) > 0 {
			conds = append(conds, "("+strings.Join(parts, " OR ")+")")
		}
	}
	for _, kv := range []struct{ col, v string }{
		{"outcome", f.Outcome}, {"provider", f.Provider}, {"auth_id", f.AuthID}, {"model", f.Model},
		{"session_id", f.SessionID}, {"api_key_hash", f.APIKeyHash}, {"machine", f.Machine},
		{"harness", f.Harness}, {"client_mode", f.ClientMode},
	} {
		if kv.v != "" {
			add(kv.col+" = $%d", kv.v)
		}
	}
	return " WHERE " + strings.Join(conds, " AND "), args
}

// Counts aggregates outcomes and tokens. Token fields use the normalised accounting: prompt
// tokens = uncached_input + cache_read + cache_write, cache hits = cache_read, output =
// output_non_reasoning + reasoning_tokens.
type Counts struct {
	Attempts           int64   `json:"attempts"`
	Requests           int64   `json:"requests"`
	Success            int64   `json:"success"`
	Failed             int64   `json:"failed"`
	Canceled           int64   `json:"canceled"`
	UncachedInput      int64   `json:"uncached_input"`
	CacheRead          int64   `json:"cache_read"`
	CacheWrite         int64   `json:"cache_write"`
	OutputNonReasoning int64   `json:"output_non_reasoning"`
	ReasoningTokens    int64   `json:"reasoning_tokens"`
	OutputTokens       int64   `json:"output_tokens"`
	PromptTokens       int64   `json:"prompt_tokens"`
	TotalTokens        int64   `json:"total_tokens"`
	CacheHitTokens     int64   `json:"cache_hit_tokens"`
	CacheHitRatio      float64 `json:"cache_hit_ratio"`
	Incomplete         int64   `json:"incomplete_accounting"`
	AvgLatencyMs       float64 `json:"avg_latency_ms"`
	P50TTFTMs          float64 `json:"p50_ttft_ms"`
	P95TTFTMs          float64 `json:"p95_ttft_ms"`
	// Deprecated aliases kept for the v1 UI: input_tokens = prompt tokens,
	// cache_creation = cache_write.
	InputTokens   int64 `json:"input_tokens"`
	CacheCreation int64 `json:"cache_creation"`
	CachedTokens  int64 `json:"cached_tokens"`
}

const countsSelect = `
	count(*),
	count(DISTINCT COALESCE(NULLIF(trace_id, ''), NULLIF(request_id, ''), import_ref)),
	count(*) FILTER (WHERE outcome = 'success'),
	count(*) FILTER (WHERE outcome = 'failed'),
	count(*) FILTER (WHERE outcome = 'canceled'),
	COALESCE(sum(uncached_input), 0),
	COALESCE(sum(cache_read), 0),
	COALESCE(sum(cache_write), 0),
	COALESCE(sum(output_non_reasoning), 0),
	COALESCE(sum(reasoning_tokens), 0),
	COALESCE(sum(total_tokens), 0),
	count(*) FILTER (WHERE accounting_quality NOT IN ('complete', '')),
	COALESCE(avg(latency_ms) FILTER (WHERE outcome = 'success' AND latency_ms > 0), 0),
	COALESCE(percentile_cont(0.5) WITHIN GROUP (ORDER BY ttft_ms) FILTER (WHERE outcome = 'success' AND ttft_ms > 0), 0),
	COALESCE(percentile_cont(0.95) WITHIN GROUP (ORDER BY ttft_ms) FILTER (WHERE outcome = 'success' AND ttft_ms > 0), 0)`

func (c *Counts) scanTargets() []any {
	return []any{&c.Attempts, &c.Requests, &c.Success, &c.Failed, &c.Canceled, &c.UncachedInput,
		&c.CacheRead, &c.CacheWrite, &c.OutputNonReasoning, &c.ReasoningTokens, &c.TotalTokens,
		&c.Incomplete, &c.AvgLatencyMs, &c.P50TTFTMs, &c.P95TTFTMs}
}

func (c *Counts) finish() {
	c.PromptTokens = c.UncachedInput + c.CacheRead + c.CacheWrite
	c.OutputTokens = c.OutputNonReasoning + c.ReasoningTokens
	c.CacheHitTokens = c.CacheRead
	if c.PromptTokens > 0 {
		c.CacheHitRatio = float64(c.CacheHitTokens) / float64(c.PromptTokens)
	}
	c.InputTokens, c.CacheCreation, c.CachedTokens = c.PromptTokens, c.CacheWrite, c.CacheRead
}

// Bucket is one time slice of the summary series.
type Bucket struct {
	Start time.Time `json:"start"`
	Counts
}

// Group is a breakdown row.
type Group struct {
	Key      string `json:"key"`
	Label    string `json:"label"`
	Provider string `json:"provider"`
	Counts
}

// Sessions summarises session affinity: sessions seen, and how many were served by more than
// one account (each switch costs a prompt-cache rebuild).
type Sessions struct {
	Total     int64 `json:"total"`
	MultiAuth int64 `json:"multi_auth"`
}

// Summary is the payload of the summary endpoint.
type Summary struct {
	From          time.Time        `json:"from"`
	To            time.Time        `json:"to"`
	BucketSeconds int64            `json:"bucket_seconds"`
	Totals        Counts           `json:"totals"`
	Buckets       []Bucket         `json:"buckets"`
	ByAuth        []Group          `json:"by_auth"`
	ByModel       []Group          `json:"by_model"`
	BySource      []Group          `json:"by_source"`
	Sessions      Sessions         `json:"sessions"`
	Writer        map[string]int64 `json:"writer"`
}

// Summarize aggregates rows matching f.
func (s *Store) Summarize(ctx context.Context, f Filter, bucket time.Duration) (*Summary, error) {
	if bucket < time.Minute {
		bucket = time.Minute
	}
	out := &Summary{From: f.From, To: f.To, BucketSeconds: int64(bucket.Seconds()), Writer: s.Stats()}
	where, args := f.where()
	from := ` FROM ` + s.table() + where

	if err := s.db.QueryRowContext(ctx, `SELECT`+countsSelect+from, args...).
		Scan(out.Totals.scanTargets()...); err != nil {
		return nil, fmt.Errorf("totals: %w", err)
	}
	out.Totals.finish()

	bucketArg := len(args) + 1
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`SELECT date_bin(make_interval(secs => $%d), ts, $1) AS b,`,
		bucketArg)+countsSelect+from+` GROUP BY b ORDER BY b`, append(args, bucket.Seconds())...)
	if err != nil {
		return nil, fmt.Errorf("buckets: %w", err)
	}
	for rows.Next() {
		var b Bucket
		if errScan := rows.Scan(append([]any{&b.Start}, b.scanTargets()...)...); errScan != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("buckets: %w", errScan)
		}
		b.finish()
		out.Buckets = append(out.Buckets, b)
	}
	_ = rows.Close()

	if out.ByAuth, err = s.groups(ctx, from, args, `auth_id`, `max(auth_label)`); err != nil {
		return nil, fmt.Errorf("by auth: %w", err)
	}
	if out.ByModel, err = s.groups(ctx, from, args, `model`, `max(model)`); err != nil {
		return nil, fmt.Errorf("by model: %w", err)
	}
	if out.BySource, err = s.groups(ctx, from, args, `source`, `max(source)`); err != nil {
		return nil, fmt.Errorf("by source: %w", err)
	}
	if err = s.db.QueryRowContext(ctx, `SELECT count(*), count(*) FILTER (WHERE auths > 1) FROM (
		SELECT session_id, count(DISTINCT auth_id) AS auths`+from+` AND session_id <> ''
		GROUP BY session_id) s`, args...).Scan(&out.Sessions.Total, &out.Sessions.MultiAuth); err != nil {
		return nil, fmt.Errorf("sessions: %w", err)
	}
	return out, nil
}

func (s *Store) groups(ctx context.Context, from string, args []any, keyExpr, labelExpr string) ([]Group, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+keyExpr+`, `+labelExpr+`, max(provider),`+
		countsSelect+from+` GROUP BY `+keyExpr+` ORDER BY count(*) DESC LIMIT 100`, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []Group
	for rows.Next() {
		var g Group
		if errScan := rows.Scan(append([]any{&g.Key, &g.Label, &g.Provider}, g.scanTargets()...)...); errScan != nil {
			return nil, errScan
		}
		g.finish()
		out = append(out, g)
	}
	return out, rows.Err()
}

// Item is one row as returned by the list endpoint.
type Item struct {
	ID int64 `json:"id"`
	Row
}

// List returns rows newest first, paged by id.
func (s *Store) List(ctx context.Context, f Filter, beforeID int64, limit int) ([]Item, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	where, args := f.where()
	if beforeID > 0 {
		args = append(args, beforeID)
		where += fmt.Sprintf(" AND id < $%d", len(args))
	}
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, `SELECT id, `+insertColumns+` FROM `+s.table()+where+
		fmt.Sprintf(` ORDER BY id DESC LIMIT $%d`, len(args)), args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	items := []Item{}
	for rows.Next() {
		var it Item
		if errScan := rows.Scan(append([]any{&it.ID}, rowFields(&it.Row)...)...); errScan != nil {
			return nil, errScan
		}
		it.loadImportRef()
		items = append(items, it)
	}
	return items, rows.Err()
}

// RollupDims are the dimensions a rollup may group by.
var RollupDims = map[string]bool{
	"source": true, "provider": true, "model": true, "response_model": true, "auth_id": true,
	"api_key_hash": true, "machine": true, "outcome": true, "accounting_quality": true,
	"session_id": true, "harness": true, "client_mode": true,
}

// RollupRow is one cell of the rollup cube.
type RollupRow struct {
	Start    time.Time         `json:"start"`
	Dims     map[string]string `json:"dims"`
	Attempts int64             `json:"attempts"`
	Success  int64             `json:"success"`
	Failed   int64             `json:"failed"`
	Canceled int64             `json:"canceled"`
	// Normalised tokens; see Counts.
	UncachedInput      int64 `json:"uncached_input"`
	CacheRead          int64 `json:"cache_read"`
	CacheWrite         int64 `json:"cache_write"`
	OutputNonReasoning int64 `json:"output_non_reasoning"`
	ReasoningTokens    int64 `json:"reasoning_tokens"`
	TotalTokens        int64 `json:"total_tokens"`
	Incomplete         int64 `json:"incomplete_accounting"`
}

// Rollup groups rows into calendar buckets ("hour" or "day") in time zone tz, by dims.
// Buckets start at local midnight / local hour boundaries (IST is +05:30, so its hours differ
// from UTC hours).
func (s *Store) Rollup(ctx context.Context, f Filter, unit, tz string, dims []string, maxRows int) ([]RollupRow, bool, error) {
	if unit != "hour" && unit != "day" {
		return nil, false, fmt.Errorf("bucket must be 1h or 1d")
	}
	for _, d := range dims {
		if !RollupDims[d] {
			return nil, false, fmt.Errorf("unknown dim %q", d)
		}
	}
	where, args := f.where()
	args = append(args, tz)
	tzArg := len(args)
	bucketExpr := fmt.Sprintf("(date_trunc('%s', ts AT TIME ZONE $%d) AT TIME ZONE $%d)", unit, tzArg, tzArg)
	cols := []string{bucketExpr + " AS b"}
	group := []string{"b"}
	for _, d := range dims {
		cols = append(cols, d)
		group = append(group, d)
	}
	args = append(args, maxRows+1)
	q := `SELECT ` + strings.Join(cols, ", ") + `,
		count(*), count(*) FILTER (WHERE outcome = 'success'), count(*) FILTER (WHERE outcome = 'failed'),
		count(*) FILTER (WHERE outcome = 'canceled'), COALESCE(sum(uncached_input), 0), COALESCE(sum(cache_read), 0),
		COALESCE(sum(cache_write), 0), COALESCE(sum(output_non_reasoning), 0), COALESCE(sum(reasoning_tokens), 0),
		COALESCE(sum(total_tokens), 0), count(*) FILTER (WHERE accounting_quality NOT IN ('complete', ''))
		FROM ` + s.table() + where + ` GROUP BY ` + strings.Join(group, ", ") +
		` ORDER BY ` + strings.Join(group, ", ") + fmt.Sprintf(` LIMIT $%d`, len(args))
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = rows.Close() }()
	out := []RollupRow{}
	for rows.Next() {
		var r RollupRow
		vals := make([]string, len(dims))
		targets := []any{&r.Start}
		for i := range vals {
			targets = append(targets, &vals[i])
		}
		targets = append(targets, &r.Attempts, &r.Success, &r.Failed, &r.Canceled, &r.UncachedInput,
			&r.CacheRead, &r.CacheWrite, &r.OutputNonReasoning, &r.ReasoningTokens, &r.TotalTokens, &r.Incomplete)
		if err = rows.Scan(targets...); err != nil {
			return nil, false, err
		}
		r.Dims = make(map[string]string, len(dims))
		for i, d := range dims {
			r.Dims[d] = vals[i]
		}
		out = append(out, r)
	}
	truncated := len(out) > maxRows
	if truncated {
		out = out[:maxRows]
	}
	return out, truncated, rows.Err()
}

// SessionTotal is one session's usage across a time range.
type SessionTotal struct {
	SessionID string    `json:"session_id"`
	FirstTS   time.Time `json:"first_ts"`
	LastTS    time.Time `json:"last_ts"`
	Models    []string  `json:"models"`
	AuthIDs   []string  `json:"auth_ids"`
	Machines  []string  `json:"machines"`
	Harnesses []string  `json:"harnesses"`
	Sources   []string  `json:"sources"`
	KeyHashes []string  `json:"api_key_hashes"`
	Attempts  int64     `json:"attempts"`
	Success   int64     `json:"success"`
	Failed    int64     `json:"failed"`
	Canceled  int64     `json:"canceled"`
	// Normalised tokens; see Counts.
	UncachedInput      int64 `json:"uncached_input"`
	CacheRead          int64 `json:"cache_read"`
	CacheWrite         int64 `json:"cache_write"`
	OutputNonReasoning int64 `json:"output_non_reasoning"`
	ReasoningTokens    int64 `json:"reasoning_tokens"`
	TotalTokens        int64 `json:"total_tokens"`
	Incomplete         int64 `json:"incomplete_accounting"`
}

// SessionTotals returns per-session totals for rows matching f, largest total_tokens first (or
// most recent first with order "recent"). Rows without a session id are skipped. Pricing per
// model is the caller's job: use a rollup with dims=session_id,model for that.
func (s *Store) SessionTotals(ctx context.Context, f Filter, order string, limit int) ([]SessionTotal, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	where, args := f.where()
	orderBy := "sum(total_tokens) DESC"
	if order == "recent" {
		orderBy = "max(ts) DESC"
	}
	args = append(args, limit)
	agg := func(col string) string {
		return "COALESCE(array_agg(DISTINCT " + col + ") FILTER (WHERE " + col + " <> ''), '{}')"
	}
	q := `SELECT session_id, min(ts), max(ts), ` + agg("model") + `, ` + agg("auth_id") + `, ` +
		agg("machine") + `, ` + agg("source") + `, ` + agg("api_key_hash") + `, ` + agg("harness") + `,
		count(*), count(*) FILTER (WHERE outcome = 'success'), count(*) FILTER (WHERE outcome = 'failed'),
		count(*) FILTER (WHERE outcome = 'canceled'), COALESCE(sum(uncached_input), 0), COALESCE(sum(cache_read), 0),
		COALESCE(sum(cache_write), 0), COALESCE(sum(output_non_reasoning), 0), COALESCE(sum(reasoning_tokens), 0),
		COALESCE(sum(total_tokens), 0), count(*) FILTER (WHERE accounting_quality NOT IN ('complete', ''))
		FROM ` + s.table() + where + ` AND session_id <> '' GROUP BY session_id ORDER BY ` + orderBy +
		fmt.Sprintf(` LIMIT $%d`, len(args))
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := []SessionTotal{}
	for rows.Next() {
		var r SessionTotal
		var models, auths, machines, sources, keys, harnesses pgTextArray
		if err = rows.Scan(&r.SessionID, &r.FirstTS, &r.LastTS, &models, &auths, &machines, &sources, &keys, &harnesses,
			&r.Attempts, &r.Success, &r.Failed, &r.Canceled, &r.UncachedInput, &r.CacheRead, &r.CacheWrite,
			&r.OutputNonReasoning, &r.ReasoningTokens, &r.TotalTokens, &r.Incomplete); err != nil {
			return nil, err
		}
		r.Models, r.AuthIDs, r.Machines, r.Sources, r.KeyHashes, r.Harnesses = models, auths, machines, sources, keys, harnesses
		out = append(out, r)
	}
	return out, rows.Err()
}

// pgTextArray scans a Postgres text[] in its text form ("{a,b,\"c d\"}").
type pgTextArray []string

func (a *pgTextArray) Scan(src any) error {
	var s string
	switch v := src.(type) {
	case nil:
		*a = []string{}
		return nil
	case []byte:
		s = string(v)
	case string:
		s = v
	default:
		return fmt.Errorf("pgTextArray: unsupported %T", src)
	}
	s = strings.TrimSuffix(strings.TrimPrefix(s, "{"), "}")
	out := []string{}
	var cur strings.Builder
	inQuote, escaped, seen := false, false, false
	for _, r := range s {
		switch {
		case escaped:
			cur.WriteRune(r)
			escaped = false
		case r == '\\':
			escaped = true
		case r == '"':
			inQuote = !inQuote
			seen = true
		case r == ',' && !inQuote:
			out = append(out, cur.String())
			cur.Reset()
			seen = false
		default:
			cur.WriteRune(r)
			seen = true
		}
	}
	if seen || cur.Len() > 0 {
		out = append(out, cur.String())
	}
	*a = out
	return nil
}
