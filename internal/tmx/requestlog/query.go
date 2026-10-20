package requestlog

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// Prompt-cache accounting differs by provider: Anthropic reports cache reads/writes on top of
// uncached input, OpenAI reports cached tokens as a subset of input. These expressions give
// "prompt tokens" and "prompt tokens served from cache" on one scale for both.
const (
	sqlPromptTokens = `CASE WHEN provider = 'claude' THEN input_tokens + cache_read + cache_creation ELSE input_tokens END`
	sqlCacheHits    = `CASE WHEN provider = 'claude' THEN cache_read ELSE GREATEST(cached_tokens, cache_read) END`
)

// Counts aggregates outcomes and tokens for a slice of rows.
type Counts struct {
	Attempts        int64   `json:"attempts"`
	Requests        int64   `json:"requests"`
	Success         int64   `json:"success"`
	Failed          int64   `json:"failed"`
	Canceled        int64   `json:"canceled"`
	InputTokens     int64   `json:"input_tokens"`
	OutputTokens    int64   `json:"output_tokens"`
	ReasoningTokens int64   `json:"reasoning_tokens"`
	CacheRead       int64   `json:"cache_read"`
	CacheCreation   int64   `json:"cache_creation"`
	CachedTokens    int64   `json:"cached_tokens"`
	TotalTokens     int64   `json:"total_tokens"`
	PromptTokens    int64   `json:"prompt_tokens"`
	CacheHitTokens  int64   `json:"cache_hit_tokens"`
	CacheHitRatio   float64 `json:"cache_hit_ratio"`
	AvgLatencyMs    float64 `json:"avg_latency_ms"`
	P50TTFTMs       float64 `json:"p50_ttft_ms"`
	P95TTFTMs       float64 `json:"p95_ttft_ms"`
}

const countsSelect = `
	count(*),
	count(DISTINCT COALESCE(NULLIF(trace_id, ''), request_id)),
	count(*) FILTER (WHERE outcome = 'success'),
	count(*) FILTER (WHERE outcome = 'failed'),
	count(*) FILTER (WHERE outcome = 'canceled'),
	COALESCE(sum(input_tokens), 0),
	COALESCE(sum(output_tokens), 0),
	COALESCE(sum(reasoning_tokens), 0),
	COALESCE(sum(cache_read), 0),
	COALESCE(sum(cache_creation), 0),
	COALESCE(sum(cached_tokens), 0),
	COALESCE(sum(total_tokens), 0),
	COALESCE(sum(` + sqlPromptTokens + `), 0),
	COALESCE(sum(` + sqlCacheHits + `), 0),
	COALESCE(avg(latency_ms) FILTER (WHERE outcome = 'success'), 0),
	COALESCE(percentile_cont(0.5) WITHIN GROUP (ORDER BY ttft_ms) FILTER (WHERE outcome = 'success' AND ttft_ms > 0), 0),
	COALESCE(percentile_cont(0.95) WITHIN GROUP (ORDER BY ttft_ms) FILTER (WHERE outcome = 'success' AND ttft_ms > 0), 0)`

func (c *Counts) scanTargets() []any {
	return []any{&c.Attempts, &c.Requests, &c.Success, &c.Failed, &c.Canceled, &c.InputTokens,
		&c.OutputTokens, &c.ReasoningTokens, &c.CacheRead, &c.CacheCreation, &c.CachedTokens,
		&c.TotalTokens, &c.PromptTokens, &c.CacheHitTokens, &c.AvgLatencyMs, &c.P50TTFTMs,
		&c.P95TTFTMs}
}

func (c *Counts) finish() {
	if c.PromptTokens > 0 {
		c.CacheHitRatio = float64(c.CacheHitTokens) / float64(c.PromptTokens)
	}
}

// Bucket is one time slice of the summary series.
type Bucket struct {
	Start time.Time `json:"start"`
	Counts
}

// Group is a per-account or per-model breakdown row.
type Group struct {
	Key      string `json:"key"`
	Label    string `json:"label"`
	Provider string `json:"provider"`
	Counts
}

// Sessions summarises session affinity: how many client sessions were seen and how many were
// served by more than one account (each switch costs a prompt-cache rebuild).
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
	Sessions      Sessions         `json:"sessions"`
	Writer        map[string]int64 `json:"writer"`
}

// Summarize aggregates rows with ts in [from, to).
func (s *Store) Summarize(ctx context.Context, from, to time.Time, bucket time.Duration) (*Summary, error) {
	if bucket < time.Minute {
		bucket = time.Minute
	}
	out := &Summary{From: from, To: to, BucketSeconds: int64(bucket.Seconds()), Writer: s.Stats()}
	where := ` FROM ` + s.table() + ` WHERE ts >= $1 AND ts < $2`

	if err := s.db.QueryRowContext(ctx, `SELECT`+countsSelect+where, from, to).
		Scan(out.Totals.scanTargets()...); err != nil {
		return nil, fmt.Errorf("totals: %w", err)
	}
	out.Totals.finish()

	rows, err := s.db.QueryContext(ctx, `SELECT date_bin(make_interval(secs => $3), ts, $1) AS b,`+
		countsSelect+where+` GROUP BY b ORDER BY b`, from, to, bucket.Seconds())
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

	if out.ByAuth, err = s.groups(ctx, where, from, to,
		`auth_id`, `max(auth_label)`); err != nil {
		return nil, fmt.Errorf("by auth: %w", err)
	}
	if out.ByModel, err = s.groups(ctx, where, from, to, `model`, `max(model)`); err != nil {
		return nil, fmt.Errorf("by model: %w", err)
	}

	if err = s.db.QueryRowContext(ctx, `SELECT count(*), count(*) FILTER (WHERE auths > 1) FROM (
		SELECT session_id, count(DISTINCT auth_id) AS auths`+where+` AND session_id <> ''
		GROUP BY session_id) s`, from, to).Scan(&out.Sessions.Total, &out.Sessions.MultiAuth); err != nil {
		return nil, fmt.Errorf("sessions: %w", err)
	}
	return out, nil
}

func (s *Store) groups(ctx context.Context, where string, from, to time.Time, keyExpr, labelExpr string) ([]Group, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+keyExpr+`, `+labelExpr+`, max(provider),`+
		countsSelect+where+` GROUP BY `+keyExpr+` ORDER BY count(*) DESC LIMIT 100`, from, to)
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

// ListFilter selects rows for the list endpoint. Empty fields don't filter.
type ListFilter struct {
	From      time.Time
	To        time.Time
	Outcome   string
	Provider  string
	AuthID    string
	Model     string
	SessionID string
	BeforeID  int64
	Limit     int
}

// Item is one row as returned by the list endpoint.
type Item struct {
	ID int64 `json:"id"`
	Row
}

// List returns rows newest first, paged by id.
func (s *Store) List(ctx context.Context, f ListFilter) ([]Item, error) {
	if f.Limit <= 0 || f.Limit > 500 {
		f.Limit = 100
	}
	conds := []string{"ts >= $1", "ts < $2"}
	args := []any{f.From, f.To}
	add := func(cond string, v any) {
		args = append(args, v)
		conds = append(conds, fmt.Sprintf(cond, len(args)))
	}
	if f.Outcome != "" {
		add("outcome = $%d", f.Outcome)
	}
	if f.Provider != "" {
		add("provider = $%d", f.Provider)
	}
	if f.AuthID != "" {
		add("auth_id = $%d", f.AuthID)
	}
	if f.Model != "" {
		add("model = $%d", f.Model)
	}
	if f.SessionID != "" {
		add("session_id = $%d", f.SessionID)
	}
	if f.BeforeID > 0 {
		add("id < $%d", f.BeforeID)
	}
	args = append(args, f.Limit)
	q := `SELECT id, ` + insertColumns + ` FROM ` + s.table() + ` WHERE ` +
		strings.Join(conds, " AND ") + fmt.Sprintf(` ORDER BY id DESC LIMIT $%d`, len(args))
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	items := []Item{}
	for rows.Next() {
		var it Item
		r := &it.Row
		if errScan := rows.Scan(&it.ID, &r.TS, &r.RequestID, &r.TraceID, &r.SessionID, &r.Provider,
			&r.Executor, &r.Model, &r.Alias, &r.ResponseModel, &r.AuthID, &r.AuthIndex, &r.AuthLabel,
			&r.AuthType, &r.APIKeyHash, &r.AccessProvider, &r.ClientIP, &r.UserAgent, &r.Stream,
			&r.Outcome, &r.StatusCode, &r.Error, &r.LatencyMs, &r.TTFTMs, &r.InputTokens,
			&r.OutputTokens, &r.ReasoningTokens, &r.CachedTokens, &r.CacheRead, &r.CacheCreation,
			&r.TotalTokens, &r.ReasoningEffort, &r.ServiceTier); errScan != nil {
			return nil, errScan
		}
		items = append(items, it)
	}
	return items, rows.Err()
}
