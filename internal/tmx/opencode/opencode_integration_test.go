package opencode_test

import (
	"context"
	"fmt"
	"math"
	"os"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/opencode"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/requestlog"
)

// Readings over real request-log rows (TMX_REQUESTLOG_TEST_DSN, a throwaway Postgres).
func TestReadingsFromRequestLog(t *testing.T) {
	dsn := os.Getenv("TMX_REQUESTLOG_TEST_DSN")
	if dsn == "" {
		t.Skip("TMX_REQUESTLOG_TEST_DSN not set")
	}
	ctx := context.Background()
	schema := fmt.Sprintf("tmx_test_oc_%d", time.Now().UnixNano())
	rl, err := requestlog.Open(ctx, dsn, schema, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = rl.DB().Exec("DROP SCHEMA " + schema + " CASCADE")
		rl.Close()
	})
	now := time.Now().UTC().Truncate(time.Second)
	f := func(v float64) *float64 { return &v }
	type r struct {
		ago             time.Duration
		provider, model string
		allowance       *float64
	}
	row := func(ago time.Duration, provider, model string, allowance *float64) r {
		return r{ago, provider, model, allowance}
	}
	rows := []r{
		row(time.Hour, "opencode-go", "kimi-k3", f(1)),         // in 5h, 7d, 30d
		row(4*time.Hour, "opencode-go", "kimi-k3", f(0.5)),     // in 5h, 7d, 30d
		row(6*time.Hour, "opencode-go", "kimi-k3", f(2)),       // 7d, 30d
		row(10*24*time.Hour, "opencode-go", "kimi-k3", f(4)),   // 30d
		row(40*24*time.Hour, "opencode-go", "kimi-k3", f(100)), // outside every window
		row(time.Hour, "opencode-go", "glm-5.3", f(0.3)),       // no cap under the plan
		row(time.Hour, "opencode-zen", "kimi-k3", nil),         // another entry, no allowance
		row(time.Hour, "opencode-go", "kimi-k3", nil),          // a failed request: nothing used
		row(-time.Hour, "opencode-go", "kimi-k3", f(50)),       // future clock skew: ignored
	}
	for _, x := range rows {
		if _, err = rl.DB().ExecContext(ctx, `INSERT INTO `+schema+`.requests (ts, provider, model, outcome, allowance_usd)
			VALUES ($1, $2, $3, 'success', $4)`, now.Add(-x.ago), x.provider, x.model, x.allowance); err != nil {
			t.Fatal(err)
		}
	}
	store, err := opencode.NewStore(ctx, rl.DB(), schema)
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Put(ctx, opencode.Settings{Entry: "OpenCode-Go", Plan: "go",
		Plans: map[string]map[string]float64{"go": {"kimi-k3": 15}, "go-plus": {"kimi-k3": 60}}})
	if err != nil {
		t.Fatal(err)
	}
	subs, err := store.List(ctx)
	if err != nil || len(subs) != 1 || subs[0].Entry != "opencode-go" {
		t.Fatalf("list = %+v %v", subs, err)
	}
	got, err := store.Readings(ctx, st, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Model != "glm-5.3" || got[1].Model != "kimi-k3" {
		t.Fatalf("readings = %+v", got)
	}
	k := got[1].Windows
	for i, want := range []float64{1.5, 3.5, 7.5} {
		if math.Abs(k[i].UsedUSD-want) > 1e-9 {
			t.Errorf("kimi %s used %v, want %v", k[i].Window, k[i].UsedUSD, want)
		}
	}
	if *k[0].UsedPct != 50 || k[0].Status != opencode.StatusAllowed {
		t.Errorf("kimi 5h = %v %s", *k[0].UsedPct, k[0].Status)
	}
	if got[0].Windows[0].Status != opencode.StatusUnknownCap || got[0].Windows[0].UsedPct != nil {
		t.Errorf("glm = %+v", got[0].Windows[0])
	}
	// Switching the plan is a setting: the same usage reads against Go Plus caps.
	st.Plan = "go-plus"
	got, _ = store.Readings(ctx, st, now)
	if math.Abs(*got[1].Windows[2].UsedPct-12.5) > 1e-9 {
		t.Errorf("go-plus 30d = %v, want 12.5", *got[1].Windows[2].UsedPct)
	}
	if ok, errD := store.Delete(ctx, "opencode-go"); !ok || errD != nil {
		t.Fatalf("delete = %v %v", ok, errD)
	}
}
