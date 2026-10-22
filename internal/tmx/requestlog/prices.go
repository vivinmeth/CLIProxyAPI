package requestlog

// List-price cost per row (tokenmaxx fork). Every row is priced when its tokens are written, with
// the catalog snapshot loaded at that moment, and keeps that cost: a later price change or a model
// leaving the catalog never rewrites history. Snapshots of the models.dev catalog are kept in
// <schema>.price_catalogs (fetched daily; the latest is loaded at start), so pricing works offline
// and every row's priced_at names the snapshot that priced it. Snapshots are never deleted; a
// fetch identical to the latest stored one (same source and catalog) is not stored again.
//
// Rows written while no catalog is loaded stay NULL (price_source IS NULL) and are priced by the
// hourly pass; rows of models the catalog doesn't know are "unpriced" and retried once a day.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/pricing"
	log "github.com/sirupsen/logrus"
)

const (
	pricesCatalogURL = "https://models.dev/api.json"
	pricesFetchEvery = 24 * time.Hour
	pricesPassEvery  = time.Hour
	pricesBatch      = 2000
)

type priceSnapshot struct {
	cat       pricing.Catalog
	fetchedAt time.Time // the stored snapshot's key (rows' priced_at)
	checkedAt time.Time // last time models.dev confirmed it (in memory; fetchedAt after a restart)
}

// priceRow fills r's cost columns from snap (nil snap leaves the row pending).
func priceRow(r *Row, snap *priceSnapshot) {
	if snap == nil {
		return
	}
	at := snap.fetchedAt
	r.PricedAt = &at
	c, ok := snap.cat.Price(pricing.Usage{
		Provider: r.Provider, Model: r.Model, Uncached: r.UncachedInput, CacheRead: r.CacheRead,
		CacheWrite: r.CacheWrite, CacheWrite1h: r.CacheWrite1h, Output: r.OutputNonReasoning + r.ReasoningTokens,
		Speed: r.Speed, ServiceTier: r.ServiceTier,
	})
	if !ok {
		src := "unpriced"
		r.PriceSource, r.PricedAs = &src, ""
		r.CostInput, r.CostCacheRead, r.CostCacheWrite, r.CostOutput, r.CostUSD = nil, nil, nil, nil, nil
		return
	}
	src := c.Source
	r.PriceSource, r.PricedAs = &src, c.PricedAs
	r.CostInput, r.CostCacheRead, r.CostCacheWrite, r.CostOutput, r.CostUSD =
		&c.Input, &c.CacheRead, &c.CacheWrite, &c.Output, &c.Total
}

func (s *Store) priceSnap() *priceSnapshot { return s.prices.Load() }

// pricesLoop loads the latest stored snapshot, refreshes it from models.dev once a day and prices
// pending rows every hour.
func (s *Store) pricesLoop() {
	ctx := context.Background()
	if snap, err := s.loadSnapshot(ctx); err != nil {
		log.WithError(err).Warn("tmx request log: loading the stored price catalog failed")
	} else if snap != nil {
		s.prices.Store(snap)
	}
	retryUnpriced := true
	for {
		if snap := s.priceSnap(); !pricesFetchOff() && (snap == nil || time.Since(snap.checkedAt) >= pricesFetchEvery) {
			if fresh, err := s.fetchSnapshot(ctx); err != nil {
				log.WithError(err).Warn("tmx request log: price catalog unavailable; keeping the current one")
			} else {
				s.prices.Store(fresh)
				retryUnpriced = true
			}
		}
		if n, err := s.pricePending(ctx, retryUnpriced); err != nil {
			log.WithError(err).Warn("tmx request log: pricing pending rows failed")
		} else {
			retryUnpriced = false
			if n > 0 {
				log.Infof("tmx request log: priced %d rows", n)
			}
		}
		select {
		case <-s.stopPrices:
			return
		case <-time.After(pricesPassEvery):
		}
	}
}

func pricesFetchOff() bool {
	return strings.EqualFold(strings.TrimSpace(os.Getenv("TMX_PRICES")), "off")
}

func (s *Store) loadSnapshot(ctx context.Context) (*priceSnapshot, error) {
	var at time.Time
	var body []byte
	err := s.db.QueryRowContext(ctx, `SELECT fetched_at, catalog FROM `+s.schema+`.price_catalogs
		ORDER BY fetched_at DESC LIMIT 1`).Scan(&at, &body)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var cat pricing.Catalog
	if err = json.Unmarshal(body, &cat); err != nil {
		return nil, err
	}
	return &priceSnapshot{cat: cat, fetchedAt: at.UTC(), checkedAt: at.UTC()}, nil
}

// fetchSnapshot downloads the catalog and returns it, stored as a new snapshot unless it is
// identical to the latest stored one (then that snapshot is returned, so priced_at keeps naming it).
func (s *Store) fetchSnapshot(ctx context.Context) (*priceSnapshot, error) {
	url := pricesCatalogURL
	if v := strings.TrimSpace(os.Getenv("TMX_PRICES_CATALOG_URL")); v != "" {
		url = v
	}
	fctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	req, err := http.NewRequestWithContext(fctx, http.MethodGet, url, nil)
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
	cat, err := pricing.ParseModelsDev(raw)
	if err != nil {
		return nil, err
	}
	body, err := json.Marshal(cat)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	var latest time.Time
	var same bool
	err = s.db.QueryRowContext(ctx, `SELECT fetched_at, source = $1 AND catalog = $2::jsonb FROM `+s.schema+`.price_catalogs
		ORDER BY fetched_at DESC LIMIT 1`, url, string(body)).Scan(&latest, &same)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	if err == nil && same {
		return &priceSnapshot{cat: cat, fetchedAt: latest.UTC(), checkedAt: now}, nil
	}
	at := now.Truncate(time.Microsecond)
	if _, err = s.db.ExecContext(ctx, `INSERT INTO `+s.schema+`.price_catalogs (fetched_at, source, catalog)
		VALUES ($1, $2, $3)`, at, url, string(body)); err != nil {
		return nil, err
	}
	return &priceSnapshot{cat: cat, fetchedAt: at, checkedAt: now}, nil
}

// pricePending prices rows that have no price yet (and, when retryUnpriced, rows whose model the
// catalog didn't know). Priced rows are never repriced.
func (s *Store) pricePending(ctx context.Context, retryUnpriced bool) (int, error) {
	snap := s.priceSnap()
	if snap == nil {
		return 0, nil
	}
	cond := `price_source IS NULL`
	if retryUnpriced {
		cond = `(price_source IS NULL OR price_source = 'unpriced')`
	}
	total := 0
	lastID := int64(0)
	for {
		rows, err := s.db.QueryContext(ctx, `SELECT id, provider, model, uncached_input, cache_read, cache_write,
			cache_write_1h, output_non_reasoning, reasoning_tokens, speed, service_tier, `+pricingInputs+`
			FROM `+s.table()+` t WHERE `+cond+` AND id > $1 ORDER BY id LIMIT $2`, lastID, pricesBatch)
		if err != nil {
			return total, err
		}
		var ids []int64
		var fps []string
		var batch []Row
		for rows.Next() {
			var id int64
			var fp string
			var r Row
			if err = rows.Scan(&id, &r.Provider, &r.Model, &r.UncachedInput, &r.CacheRead, &r.CacheWrite,
				&r.CacheWrite1h, &r.OutputNonReasoning, &r.ReasoningTokens, &r.Speed, &r.ServiceTier, &fp); err != nil {
				_ = rows.Close()
				return total, err
			}
			ids = append(ids, id)
			fps = append(fps, fp)
			batch = append(batch, r)
		}
		if err = rows.Err(); err != nil {
			_ = rows.Close()
			return total, err
		}
		_ = rows.Close()
		if len(ids) == 0 {
			return total, nil
		}
		lastID = ids[len(ids)-1]
		for i := range batch {
			priceRow(&batch[i], snap)
		}
		if err = s.writePrices(ctx, ids, fps, batch, snap.fetchedAt); err != nil {
			return total, err
		}
		total += len(batch)
	}
}

// pricingInputs fingerprints what a row is priced on (columns of alias t).
const pricingInputs = `md5(concat_ws('|', t.provider, t.model, t.uncached_input, t.cache_read, t.cache_write,
	t.cache_write_1h, t.output_non_reasoning, t.reasoning_tokens, t.speed, t.service_tier))`

// writePrices stores the cost columns of already-written rows, only where they are still unpriced
// and their pricing inputs are what was priced (a row changed meanwhile is left for the next
// pass). It runs as a bulk update: the analytics trigger journals one reconcile instead of an
// entry per row (tmx_archive.analytics_journal_requests reads tmx.analytics_bulk).
func (s *Store) writePrices(ctx context.Context, ids []int64, fps []string, rows []Row, at time.Time) error {
	n := len(ids)
	inp, cr, cw, out, tot := make([]*float64, n), make([]*float64, n), make([]*float64, n), make([]*float64, n), make([]*float64, n)
	as, src := make([]string, n), make([]string, n)
	for i := range rows {
		r := &rows[i]
		inp[i], cr[i], cw[i], out[i], tot[i] = r.CostInput, r.CostCacheRead, r.CostCacheWrite, r.CostOutput, r.CostUSD
		as[i], src[i] = r.PricedAs, *r.PriceSource
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err = tx.ExecContext(ctx, `SET LOCAL tmx.analytics_bulk = 'on'`); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE `+s.table()+` AS t SET cost_input = u.ci, cost_cache_read = u.ccr,
		cost_cache_write = u.ccw, cost_output = u.co, cost_usd = u.ct, priced_as = u.pa, price_source = u.ps,
		priced_at = $10
		FROM unnest($1::bigint[], $2::float8[], $3::float8[], $4::float8[], $5::float8[], $6::float8[],
			$7::text[], $8::text[], $9::text[]) AS u(id, ci, ccr, ccw, co, ct, pa, ps, fp)
		WHERE t.id = u.id AND (t.price_source IS NULL OR t.price_source = 'unpriced') AND `+pricingInputs+` = u.fp`,
		int64Array(ids), floatArray(inp), floatArray(cr), floatArray(cw), floatArray(out), floatArray(tot),
		textArray(as), textArray(src), textArray(fps), at); err != nil {
		return err
	}
	return tx.Commit()
}

func int64Array(v []int64) string {
	parts := make([]string, len(v))
	for i, x := range v {
		parts[i] = fmt.Sprint(x)
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func floatArray(v []*float64) string {
	parts := make([]string, len(v))
	for i, x := range v {
		if x == nil {
			parts[i] = "NULL"
		} else {
			parts[i] = fmt.Sprintf("%.17g", *x)
		}
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func textArray(v []string) string {
	parts := make([]string, len(v))
	for i, x := range v {
		parts[i] = `"` + strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(x) + `"`
	}
	return "{" + strings.Join(parts, ",") + "}"
}

// SetPriceCatalogForTest installs a catalog snapshot (tests; set TMX_PRICES=off before Open so
// the loop doesn't fetch one).
func (s *Store) SetPriceCatalogForTest(cat pricing.Catalog, at time.Time) {
	s.prices.Store(&priceSnapshot{cat: cat, fetchedAt: at.UTC(), checkedAt: at.UTC()})
}

// PricePending runs one pricing pass now (rows without a price; with retryUnpriced also rows
// whose model was unknown).
func (s *Store) PricePending(ctx context.Context, retryUnpriced bool) (int, error) {
	return s.pricePending(ctx, retryUnpriced)
}
