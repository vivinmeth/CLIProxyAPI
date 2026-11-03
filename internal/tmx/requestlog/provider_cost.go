package requestlog

import (
	"context"
	"strings"
	"time"

	coreusage "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/usage"
	log "github.com/sirupsen/logrus"
)

// Provider-reported cost (tokenmaxx fork). OpenCode Go and Zen report each request's cost in the
// response (sdk/cliproxy/usage.ProviderCost). Those rows take it instead of a catalog price:
// cost_usd = list_cost (the API-equivalent value, like every other row's cost_usd), price_source
// "opencode", charged_usd = what the balance was really charged, allowance_usd = Go allowance used.

// PriceSourceProvider marks rows priced from the upstream's own report.
const PriceSourceProvider = "opencode"

// WarningGoBilledToBalance flags a Go request charged to the pay-as-you-go balance: OpenCode's
// Extra Usage is on, which the owner keeps off.
const WarningGoBilledToBalance = "go_billed_to_balance"

// opencodeGoURLMarker identifies the Go base URL (https://opencode.ai/inference/go/...).
const opencodeGoURLMarker = "/inference/go/"

func (r *Row) providerPriced() bool {
	return r.PriceSource != nil && *r.PriceSource == PriceSourceProvider
}

// applyProviderCost fills the provider-cost columns, billing and (when OpenCode gave one) the
// row's cost. baseURL is the credential's upstream base URL.
func (r *Row) applyProviderCost(pc *coreusage.ProviderCost, baseURL string) {
	if pc == nil {
		return
	}
	r.ChargedUSD, r.AllowanceUSD, r.BillingSource = pc.Charged, pc.Allowance, pc.BillingSource
	switch src := pc.BillingSource; {
	case src == "go" || strings.HasPrefix(src, "go-"): // go, go-plus, and any later Go tier
		r.Billing = BillingSubscription
	case src == "free":
		r.Billing = BillingFree
	case src == "balance" || src == "byok":
		r.Billing = BillingAPIKey
	}
	if pc.BillingSource == "balance" && strings.Contains(strings.ToLower(baseURL), opencodeGoURLMarker) {
		r.Warning = WarningGoBilledToBalance
		charged := 0.0
		if pc.Charged != nil {
			charged = *pc.Charged
		}
		log.WithFields(log.Fields{"model": r.Model, "auth": r.AuthLabel, "charged_usd": charged}).
			Warn("tmx request log: an OpenCode Go request was billed to the balance (Extra Usage is on)")
	}
	cost := pc.ListCost
	if cost == nil {
		cost = pc.Charged
	}
	if cost == nil {
		return // nothing reported: the catalog prices it as usual
	}
	v, src, at := *cost, PriceSourceProvider, r.TS
	r.CostUSD, r.PriceSource, r.PricedAs, r.PricedAt = &v, &src, r.Model, &at
	r.CostInput, r.CostCacheRead, r.CostCacheWrite, r.CostOutput = nil, nil, nil, nil
}

// ensureAllowanceIndex builds the partial index the OpenCode Go readings scan (rows with allowance),
// off the startup path: concurrently, so writes never wait, under its own generous timeout, so a big
// table or a long-running transaction (a backup) can't fail the request log's start. A build that
// was interrupted leaves an INVALID index that IF NOT EXISTS would skip forever: it is dropped and
// built again. Readings work without it, only slower.
func (s *Store) ensureAllowanceIndex() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	var valid *bool
	err := s.db.QueryRowContext(ctx, `SELECT i.indisvalid FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = $1 AND c.relname = 'requests_allowance_idx'`,
		s.schema).Scan(&valid)
	if err == nil && valid != nil && *valid {
		return
	}
	if err == nil && valid != nil && !*valid {
		if _, err = s.db.ExecContext(ctx, `DROP INDEX CONCURRENTLY IF EXISTS `+s.schema+`.requests_allowance_idx`); err != nil {
			log.WithError(err).Warn("tmx request log: dropping the invalid allowance index failed")
			return
		}
	}
	if _, err = s.db.ExecContext(ctx, `CREATE INDEX CONCURRENTLY IF NOT EXISTS requests_allowance_idx ON `+s.table()+
		` (provider, ts) WHERE allowance_usd IS NOT NULL`); err != nil {
		log.WithError(err).Warn("tmx request log: building the allowance index failed; OpenCode readings scan without it")
	}
}
