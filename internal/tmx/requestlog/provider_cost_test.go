package requestlog

import (
	"context"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/pricing"
	coreusage "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/usage"
)

func f64(v float64) *float64 { return &v }

func TestRowFromRecordUsesOpenCodeCost(t *testing.T) {
	rec := coreusage.Record{Provider: "opencode-go", Model: "kimi-k3", AuthType: "apikey",
		BaseURL: "https://opencode.ai/inference/go/openai/v1",
		Detail: coreusage.Detail{InputTokens: 10, OutputTokens: 2, ProviderCost: &coreusage.ProviderCost{
			Charged: f64(0), ListCost: f64(0.25), Allowance: f64(0.2), BillingSource: "go"}}}
	row := rowFromRecord(context.Background(), rec)
	if row.Billing != BillingSubscription || row.BillingSource != "go" || row.Warning != "" {
		t.Fatalf("billing = %q/%q warning %q", row.Billing, row.BillingSource, row.Warning)
	}
	if row.CostUSD == nil || *row.CostUSD != 0.25 || row.PriceSource == nil || *row.PriceSource != PriceSourceProvider ||
		row.ChargedUSD == nil || *row.ChargedUSD != 0 || row.AllowanceUSD == nil || *row.AllowanceUSD != 0.2 || row.PricedAt == nil {
		t.Fatalf("cost columns = %+v", row)
	}
	// The write-time catalog pass must not override a reported cost.
	snap := &priceSnapshot{cat: pricing.Catalog{}}
	priceRow(&row, snap)
	if *row.CostUSD != 0.25 || *row.PriceSource != PriceSourceProvider {
		t.Fatalf("priceRow overrode the reported cost: %v %v", *row.CostUSD, *row.PriceSource)
	}
}

func TestGoBilledToBalanceIsFlagged(t *testing.T) {
	rec := coreusage.Record{Provider: "opencode-go", Model: "glm-5.3", AuthType: "apikey",
		BaseURL: "https://opencode.ai/inference/go/openai/v1",
		Detail:  coreusage.Detail{ProviderCost: &coreusage.ProviderCost{Charged: f64(0.03), ListCost: f64(0.03), BillingSource: "balance"}}}
	row := rowFromRecord(context.Background(), rec)
	if row.Warning != WarningGoBilledToBalance || row.Billing != BillingAPIKey {
		t.Fatalf("warning %q billing %q", row.Warning, row.Billing)
	}
	// Zen billed to the balance is expected: no warning.
	rec.Provider, rec.BaseURL = "opencode-zen", "https://opencode.ai/inference/openai/v1"
	if row := rowFromRecord(context.Background(), rec); row.Warning != "" {
		t.Fatalf("Zen balance row flagged: %q", row.Warning)
	}
}

func TestRowsWithoutProviderCostUnchanged(t *testing.T) {
	row := rowFromRecord(context.Background(), coreusage.Record{Provider: "claude", Model: "claude-opus-5-5", AuthType: "oauth"})
	if row.Billing != BillingSubscription || row.PriceSource != nil || row.CostUSD != nil || row.ChargedUSD != nil ||
		row.AllowanceUSD != nil || row.BillingSource != "" || row.Warning != "" {
		t.Fatalf("claude row changed: %+v", row)
	}
	// Reported tokens but no cost fields: the catalog prices it as before.
	row = rowFromRecord(context.Background(), coreusage.Record{Provider: "opencode-zen", Model: "glm-5.3", AuthType: "apikey",
		Detail: coreusage.Detail{ProviderCost: &coreusage.ProviderCost{BillingSource: "balance"}}})
	if row.PriceSource != nil || row.CostUSD != nil {
		t.Fatalf("row without reported cost was priced: %+v", row)
	}
}

func TestLaterGoTiersBillAsSubscription(t *testing.T) {
	for src, want := range map[string]string{"go": BillingSubscription, "go-plus": BillingSubscription,
		"go-teams": BillingSubscription, "free": BillingFree, "byok": BillingAPIKey, "balance": BillingAPIKey, "gold": BillingAPIKey} {
		row := rowFromRecord(context.Background(), coreusage.Record{Provider: "opencode-go", Model: "m", AuthType: "apikey",
			Detail: coreusage.Detail{ProviderCost: &coreusage.ProviderCost{BillingSource: src}}})
		if row.Billing != want {
			t.Errorf("billing_source %q -> %q, want %q", src, row.Billing, want)
		}
	}
}
