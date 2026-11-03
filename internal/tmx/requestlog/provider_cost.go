package requestlog

import (
	"strings"

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
	switch pc.BillingSource {
	case "go", "go-plus":
		r.Billing = BillingSubscription
	case "free":
		r.Billing = BillingFree
	case "balance", "byok":
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
