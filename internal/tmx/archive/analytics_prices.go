package archive

// List prices for the usage rows (tokenmaxx fork): model_prices holds the USD-per-1M-token rates
// of every (provider, model) pair in requests, resolved against the models.dev catalog the way
// the fleet engine prices them (fleet-engine/src/usage/pricing.rs). Models the catalog doesn't
// know stay unpriced (NULL rates), never $0. The request_costs / session_usage views apply them.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	pricesCatalogURL = "https://models.dev/api.json"
	pricesRefresh    = time.Hour      // re-resolve the pairs in requests
	pricesCatalogTTL = 24 * time.Hour // re-fetch the catalog
)

var pricesSchemaSQL = []string{
	`CREATE TABLE IF NOT EXISTS model_prices (
		provider         TEXT NOT NULL,           -- as in requests.provider
		model            TEXT NOT NULL,           -- as in requests.model
		catalog_provider TEXT NOT NULL DEFAULT '',
		catalog_model    TEXT NOT NULL DEFAULT '', -- the models.dev id it is priced as; '' = unpriced
		input            DOUBLE PRECISION,        -- USD per 1M tokens; NULL = unpriced
		output           DOUBLE PRECISION,
		cache_read       DOUBLE PRECISION,
		cache_write      DOUBLE PRECISION,
		long_threshold   BIGINT,                  -- prompt tokens above which the long_* rates apply to the whole request
		long_input       DOUBLE PRECISION,
		long_output      DOUBLE PRECISION,
		long_cache_read  DOUBLE PRECISION,
		long_cache_write DOUBLE PRECISION,
		source           TEXT NOT NULL DEFAULT '', -- models.dev | local
		updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
		PRIMARY KEY (provider, model)
	)`,
}

// analyticsViewsSQL needs the requests mirror. Views are recreated on start so their definition
// follows the code.
var analyticsViewsSQL = []string{
	`DROP VIEW IF EXISTS session_usage, request_costs, request_sessions`,
	// Which extracted session (and message) a usage row belongs to. Imported rows: import_ref
	// "<harness>:<session>#<message>". Proxied rows: the captured exchange (trace_id), else the
	// client session id. canonical_session_id prefers the imported transcript over its proxied twin.
	`CREATE VIEW request_sessions AS
	SELECT r.id AS request_id, x.id AS session_id, COALESCE(s.shadowed_by, x.id) AS canonical_session_id,
		m.seq AS message_seq, x.via
	FROM requests r
	LEFT JOIN LATERAL (
		SELECT c.id, c.via FROM (
			SELECT s.id, 'import_ref' AS via, 1 AS p FROM sessions s
				WHERE r.import_ref <> '' AND s.id = split_part(r.import_ref, '#', 1)
			UNION ALL
			SELECT s.id, 'trace_id', 2 FROM exchanges e JOIN sessions s ON s.id = 'proxy:' ||
				CASE WHEN e.client_session_id <> '' THEN e.client_session_id ELSE e.session_id END
				WHERE r.trace_id <> '' AND e.trace_id = r.trace_id
			UNION ALL
			SELECT s.id, 'session_id', 3 FROM sessions s WHERE r.session_id <> '' AND s.id = 'proxy:' || r.session_id
			UNION ALL
			SELECT s.id, 'session_id', 3 FROM sessions s WHERE r.session_id <> '' AND s.alt_ids @> ARRAY[r.session_id]
			UNION ALL
			SELECT s.id, 'external_id', 4 FROM sessions s
				WHERE r.session_id <> '' AND s.kind = 'imported' AND s.external_id = r.session_id
		) c ORDER BY c.p LIMIT 1
	) x ON true
	LEFT JOIN sessions s ON s.id = x.id
	LEFT JOIN messages m ON r.import_ref <> '' AND m.session_id = x.id
		AND m.external_id = split_part(r.import_ref, '#', 2)`,
	// List-price cost per usage row (fleet's formula: prompt = uncached + cache read + cache write,
	// output includes reasoning). cost_usd is NULL when the model is unpriced.
	`CREATE VIEW request_costs AS
	SELECT r.id AS request_id, r.ts, r.source, r.harness, r.machine, r.provider, r.model, r.shadowed,
		p.catalog_model AS priced_as, r.uncached_input, r.cache_read, r.cache_write, r.output_tokens,
		t.long_context,
		COALESCE(r.uncached_input, 0) * t.input / 1e6 AS cost_input,
		COALESCE(r.cache_read, 0) * t.cache_read / 1e6 AS cost_cache_read,
		COALESCE(r.cache_write, 0) * t.cache_write / 1e6 AS cost_cache_write,
		COALESCE(r.output_tokens, 0) * t.output / 1e6 AS cost_output,
		(COALESCE(r.uncached_input, 0) * t.input + COALESCE(r.cache_read, 0) * t.cache_read +
		 COALESCE(r.cache_write, 0) * t.cache_write + COALESCE(r.output_tokens, 0) * t.output) / 1e6 AS cost_usd
	FROM requests r
	LEFT JOIN model_prices p ON p.provider = r.provider AND p.model = r.model
	LEFT JOIN LATERAL (
		SELECT l.long_context,
			CASE WHEN l.long_context THEN COALESCE(p.long_input, p.input) ELSE p.input END AS input,
			CASE WHEN l.long_context THEN COALESCE(p.long_output, p.output) ELSE p.output END AS output,
			CASE WHEN l.long_context THEN COALESCE(p.long_cache_read, p.cache_read) ELSE p.cache_read END AS cache_read,
			CASE WHEN l.long_context THEN COALESCE(p.long_cache_write, p.cache_write) ELSE p.cache_write END AS cache_write
		FROM (SELECT COALESCE(p.long_threshold IS NOT NULL AND COALESCE(r.uncached_input, 0) +
			COALESCE(r.cache_read, 0) + COALESCE(r.cache_write, 0) > p.long_threshold, false) AS long_context) l
	) t ON true`,
	// Tokens and list-price cost per session (canonical: an imported transcript absorbs its
	// proxied twin). Rows of shadowed (double-counted) proxy usage are left out.
	`CREATE VIEW session_usage AS
	SELECT rs.canonical_session_id AS session_id, count(*) AS requests,
		min(c.ts) AS first_request, max(c.ts) AS last_request,
		sum(COALESCE(c.uncached_input, 0) + COALESCE(c.cache_read, 0) + COALESCE(c.cache_write, 0)) AS prompt_tokens,
		sum(COALESCE(c.cache_read, 0)) AS cache_read_tokens, sum(COALESCE(c.output_tokens, 0)) AS output_tokens,
		sum(c.cost_usd) AS cost_usd, count(*) FILTER (WHERE c.cost_usd IS NULL) AS unpriced_requests
	FROM request_sessions rs JOIN request_costs c ON c.request_id = rs.request_id
	WHERE rs.canonical_session_id IS NOT NULL AND NOT COALESCE(c.shadowed, false)
	GROUP BY rs.canonical_session_id`,
}

type priceRate struct {
	Input      float64  `json:"input"`
	Output     float64  `json:"output"`
	CacheRead  *float64 `json:"cache_read"`
	CacheWrite *float64 `json:"cache_write"`
}

type catalogModel struct {
	priceRate
	Tiers []struct {
		Input      *float64 `json:"input"`
		Output     *float64 `json:"output"`
		CacheRead  *float64 `json:"cache_read"`
		CacheWrite *float64 `json:"cache_write"`
		Tier       struct {
			Type string `json:"type"`
			Size int64  `json:"size"`
		} `json:"tier"`
	} `json:"tiers"`
}

// priceCatalog: provider id -> model id (lower case) -> cost.
type priceCatalog map[string]map[string]catalogModel

func parsePriceCatalog(raw []byte) (priceCatalog, error) {
	var doc map[string]struct {
		Models map[string]struct {
			Cost *catalogModel `json:"cost"`
		} `json:"models"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, err
	}
	cat := priceCatalog{}
	for prov, p := range doc {
		for id, m := range p.Models {
			if m.Cost == nil || (m.Cost.Input <= 0 && m.Cost.Output <= 0) {
				continue
			}
			if cat[prov] == nil {
				cat[prov] = map[string]catalogModel{}
			}
			cat[prov][strings.ToLower(id)] = *m.Cost
		}
	}
	if len(cat) == 0 {
		return nil, fmt.Errorf("price catalog is empty")
	}
	return cat, nil
}

// catalogProvider maps the proxy's provider names to models.dev provider ids.
func catalogProvider(provider string) string {
	switch provider {
	case "claude", "anthropic":
		return "anthropic"
	case "codex", "openai":
		return "openai"
	case "gemini", "gemini-cli", "antigravity":
		return "google"
	case "xai", "grok":
		return "xai"
	}
	return provider
}

// priceCandidates lists the ids a model name may be priced as: `[1m]` suffixes and dated ids
// (-20261001) price like the base model; `vendor/` paths, `antigravity-` / `az-` prefixes,
// `-thinking` suffixes and `-0` minor versions are wrappers around a catalog id.
func priceCandidates(model string) []string {
	base := strings.ToLower(strings.TrimSpace(model))
	base, _, _ = strings.Cut(base, "[")
	out := []string{base}
	add := func(c string) {
		for _, o := range out {
			if o == c {
				return
			}
		}
		out = append(out, c)
	}
	if i := strings.LastIndexByte(base, '-'); i > 0 && len(base)-i-1 == 8 && strings.Trim(base[i+1:], "0123456789") == "" {
		add(base[:i])
	}
	bare := base[strings.LastIndexByte(base, '/')+1:]
	for _, p := range []string{"antigravity-", "az-"} {
		bare = strings.TrimPrefix(bare, p)
	}
	bare = strings.TrimSuffix(bare, "-thinking")
	add(bare)
	if s, ok := strings.CutSuffix(bare, "-0"); ok {
		add(s)
	}
	return out
}

// providers: when a model isn't under its own provider, the first-party catalogs win over
// resellers; the rest are tried alphabetically so the choice is stable.
func (c priceCatalog) providers() []string {
	first := []string{"anthropic", "openai", "google", "xai"}
	var rest []string
	for p := range c {
		if p != "anthropic" && p != "openai" && p != "google" && p != "xai" {
			rest = append(rest, p)
		}
	}
	sort.Strings(rest)
	return append(first, rest...)
}

// resolve finds the catalog entry for a usage row's provider and model.
func (c priceCatalog) resolve(provider, model string) (string, string, *catalogModel) {
	own := catalogProvider(provider)
	for _, id := range priceCandidates(model) {
		if m, ok := c[own][id]; ok {
			return own, id, &m
		}
		for _, p := range c.providers() {
			if m, ok := c[p][id]; ok {
				return p, id, &m
			}
		}
	}
	return "", "", nil
}

func (an *analytics) pricesLoop() {
	defer close(an.pricesDone)
	var cat priceCatalog
	var fetched time.Time
	for {
		if cat == nil || time.Since(fetched) >= pricesCatalogTTL {
			if c, err := fetchPriceCatalog(an.ctx); err != nil {
				log.WithError(err).Warn("tmx analytics: price catalog unavailable; keeping the stored prices")
			} else {
				cat, fetched = c, time.Now()
			}
		}
		if cat != nil {
			if err := an.refreshPrices(an.ctx, cat); err != nil && an.ctx.Err() == nil {
				log.WithError(err).Warn("tmx analytics: refreshing model prices failed")
			}
		}
		select {
		case <-an.stop:
			return
		case <-time.After(pricesRefresh):
		}
	}
}

func fetchPriceCatalog(ctx context.Context) (priceCatalog, error) {
	url := pricesCatalogURL
	if v := strings.TrimSpace(os.Getenv("TMX_PRICES_CATALOG_URL")); v != "" {
		url = v
	}
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s: %s", url, resp.Status)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return nil, err
	}
	return parsePriceCatalog(raw)
}

// refreshPrices prices every (provider, model) pair in requests.
func (an *analytics) refreshPrices(ctx context.Context, cat priceCatalog) error {
	rows, err := an.db.QueryContext(ctx, `SELECT DISTINCT COALESCE(provider, ''), COALESCE(model, '') FROM requests`)
	if err != nil {
		return err
	}
	var out [][]any
	for rows.Next() {
		var provider, model string
		if err = rows.Scan(&provider, &model); err != nil {
			_ = rows.Close()
			return err
		}
		out = append(out, priceRow(cat, provider, model))
	}
	if err = closeRows(rows); err != nil {
		return err
	}
	tx, err := an.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err = tx.ExecContext(ctx, `DELETE FROM model_prices`); err != nil {
		return err
	}
	if err = insertRows(ctx, tx, "model_prices", []string{"provider", "model", "catalog_provider", "catalog_model",
		"input", "output", "cache_read", "cache_write", "long_threshold", "long_input", "long_output",
		"long_cache_read", "long_cache_write", "source"}, out, ""); err != nil {
		return err
	}
	return tx.Commit()
}

func priceRow(cat priceCatalog, provider, model string) []any {
	row := []any{provider, model, "", "", nil, nil, nil, nil, nil, nil, nil, nil, nil, ""}
	if strings.Contains(provider, "ollama") { // local models cost nothing
		row[3], row[4], row[5], row[6], row[7], row[13] = model, 0.0, 0.0, 0.0, 0.0, "local"
		return row
	}
	cp, id, m := cat.resolve(provider, model)
	if m == nil {
		return row
	}
	// A missing cache rate falls back to the input rate (no discount assumed).
	orInput := func(v *float64, input float64) float64 {
		if v != nil {
			return *v
		}
		return input
	}
	row[2], row[3], row[13] = cp, id, "models.dev"
	row[4], row[5] = m.Input, m.Output
	row[6], row[7] = orInput(m.CacheRead, m.Input), orInput(m.CacheWrite, m.Input)
	for _, t := range m.Tiers {
		if t.Tier.Type != "context" || t.Tier.Size <= 0 {
			continue
		}
		in := orInput(t.Input, m.Input)
		row[8], row[9] = t.Tier.Size, in
		row[10] = orInput(t.Output, m.Output)
		row[11] = orInput(t.CacheRead, row[6].(float64))
		row[12] = orInput(t.CacheWrite, row[7].(float64))
		break
	}
	return row
}
