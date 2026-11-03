package requestlog

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	coreusage "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/usage"
)

// A provider-priced row survives the write path (write-time pricing, insert, scan) unchanged.
func TestProviderCostRoundTrip(t *testing.T) {
	dsn := os.Getenv("TMX_REQUESTLOG_TEST_DSN")
	if dsn == "" {
		t.Skip("TMX_REQUESTLOG_TEST_DSN not set")
	}
	ctx := context.Background()
	schema := fmt.Sprintf("tmx_test_pc_%d", time.Now().UnixNano())
	store, err := Open(ctx, dsn, schema, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = store.db.Exec("DROP SCHEMA " + schema + " CASCADE")
		store.Close()
	})
	ts := time.Now().UTC().Add(-time.Minute).Truncate(time.Second)
	row := rowFromRecord(ctx, coreusage.Record{RequestedAt: ts, Provider: "opencode-go", Model: "kimi-k3", AuthType: "apikey",
		BaseURL: "https://opencode.ai/inference/go/openai/v1",
		Detail: coreusage.Detail{InputTokens: 10, OutputTokens: 2, ProviderCost: &coreusage.ProviderCost{
			Charged: f64(0.01), ListCost: f64(0.25), Allowance: f64(0.2), BillingSource: "balance"}}})
	store.write([]Row{row})
	items, err := store.List(ctx, Filter{From: ts.Add(-time.Minute), To: ts.Add(time.Minute)}, 0, 0)
	if err != nil || len(items) != 1 {
		t.Fatalf("items = %d, %v", len(items), err)
	}
	got := items[0].Row
	if got.CostUSD == nil || *got.CostUSD != 0.25 || got.PriceSource == nil || *got.PriceSource != PriceSourceProvider ||
		got.ChargedUSD == nil || *got.ChargedUSD != 0.01 || got.AllowanceUSD == nil || *got.AllowanceUSD != 0.2 ||
		got.BillingSource != "balance" || got.Billing != BillingAPIKey || got.Warning != WarningGoBilledToBalance {
		t.Fatalf("round trip = %+v", got)
	}
	// The hourly pass leaves it alone (price_source is set).
	if n, err := store.pricePending(ctx, true); err != nil || n != 0 {
		t.Fatalf("pricePending = %d, %v", n, err)
	}
}

// The allowance index is built off the startup path, and an interrupted (INVALID) one is rebuilt.
func TestAllowanceIndexRepaired(t *testing.T) {
	dsn := os.Getenv("TMX_REQUESTLOG_TEST_DSN")
	if dsn == "" {
		t.Skip("TMX_REQUESTLOG_TEST_DSN not set")
	}
	ctx := context.Background()
	schema := fmt.Sprintf("tmx_test_ix_%d", time.Now().UnixNano())
	store, err := Open(ctx, dsn, schema, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = store.db.Exec("DROP SCHEMA " + schema + " CASCADE")
		store.Close()
	})
	valid := func() *bool {
		var v *bool
		_ = store.db.QueryRow(`SELECT i.indisvalid FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
			JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = $1 AND c.relname = 'requests_allowance_idx'`, schema).Scan(&v)
		return v
	}
	store.ensureAllowanceIndex()
	if v := valid(); v == nil || !*v {
		t.Fatalf("index after build: %v", v)
	}
	// Simulate an interrupted concurrent build.
	if _, err = store.db.Exec(`UPDATE pg_index SET indisvalid = false WHERE indexrelid = '` + schema + `.requests_allowance_idx'::regclass`); err != nil {
		t.Skipf("cannot mark the index invalid here (needs superuser): %v", err)
	}
	store.ensureAllowanceIndex()
	if v := valid(); v == nil || !*v {
		t.Fatalf("index after repair: %v", v)
	}
}
