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
func TestReadFromRequestLog(t *testing.T) {
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
		allowance, list *float64
	}
	goRows := "openai-compatible-opencode-go"
	rows := []r{
		// An older 5h window: opened 9h ago, closed 4h ago.
		{9 * time.Hour, goRows, "kimi-k3", f(2), f(0.5)},
		{6 * time.Hour, goRows, "kimi-k3", f(2), f(0.5)},
		// The open window: first request 3h ago, so it resets 2h from now.
		{3 * time.Hour, goRows, "kimi-k3", f(4), f(1)},
		{time.Hour, goRows, "glm-5.3-flash", f(1), f(1)},
		{2 * 24 * time.Hour, goRows, "kimi-k3", f(8), f(2)},     // 7d and 30d only
		{20 * 24 * time.Hour, goRows, "kimi-k3", f(10), f(2.5)}, // 30d only
		{40 * 24 * time.Hour, goRows, "kimi-k3", f(100), f(25)}, // outside every window
		{time.Hour, "openai-compatible-opencode-zen", "kimi-k3", f(9), f(9)},
		{time.Hour, goRows, "kimi-k3", nil, nil},        // a failed request: nothing used
		{-time.Hour, goRows, "kimi-k3", f(50), f(12.5)}, // future clock skew: ignored
	}
	for _, x := range rows {
		if _, err = rl.DB().ExecContext(ctx, `INSERT INTO `+schema+`.requests (ts, provider, model, outcome, allowance_usd, cost_usd, price_source)
			VALUES ($1, $2, $3, 'success', $4, $5, 'opencode')`, now.Add(-x.ago), x.provider, x.model, x.allowance, x.list); err != nil {
			t.Fatal(err)
		}
	}
	store, err := opencode.NewStore(ctx, rl.DB(), schema)
	if err != nil {
		t.Fatal(err)
	}
	caps := map[string]float64{"kimi-k3": 15, "glm-5.3-flash": 60}
	st, err := store.Put(ctx, opencode.Settings{Entry: "OpenCode-Go", Plan: "go",
		Plans: map[string]opencode.Plan{"go": {Pool: 60, Caps: caps}, "go-plus": {Caps: caps}}})
	if err != nil {
		t.Fatal(err)
	}
	got, err := store.Read(ctx, st, now)
	if err != nil {
		t.Fatal(err)
	}
	w := map[string]opencode.WindowReading{}
	for _, x := range got.Windows {
		w[x.Window] = x
	}
	near := func(a, b float64) bool { return math.Abs(a-b) < 1e-9 }
	// 5h: only the open window (4 + 1) of its 12 (20% of 60), resetting 2h from now.
	if !near(w["5h"].UsedUSD, 5) || w["5h"].ResetsAt == nil || !w["5h"].ResetsAt.Equal(now.Add(2*time.Hour)) || !near(*w["5h"].UsedPct, 5/12.0*100) {
		t.Errorf("5h = %+v (resets %v)", w["5h"], w["5h"].ResetsAt)
	}
	if !near(w["7d"].UsedUSD, 17) || w["7d"].ResetsAt != nil || !near(*w["7d"].UsedPct, 17/30.0*100) {
		t.Errorf("7d = %+v", w["7d"])
	}
	if !near(w["30d"].UsedUSD, 27) || w["30d"].Status != opencode.StatusAllowed {
		t.Errorf("30d = %+v", w["30d"])
	}
	if got.PoolSource != opencode.PoolFromSettings || len(got.Models) != 2 {
		t.Errorf("reading = %+v", got)
	}
	// Go Plus has no pool configured: taken from traffic (limit x allowance / list = 60 for both).
	st.Plan = "go-plus"
	if got, _ = store.Read(ctx, st, now); got.PoolSource != opencode.PoolFromTraffic || got.PoolUSD == nil || *got.PoolUSD != 60 {
		t.Errorf("go-plus pool = %v %s", got.PoolUSD, got.PoolSource)
	}
	// An unknown plan reads unknown, never a guess.
	st.Plan = "go-max"
	if got, _ = store.Read(ctx, st, now); got.PoolUSD != nil || got.Windows[0].Status != opencode.StatusUnknownCap || got.Windows[0].UsedPct != nil {
		t.Errorf("unknown plan = %+v", got)
	}
	// Once the open window has closed, the 5h reading is zero with no reset time.
	st.Plan = "go"
	if got, _ = store.Read(ctx, st, now.Add(3*time.Hour)); got.Windows[0].UsedUSD != 0 || got.Windows[0].ResetsAt != nil {
		t.Errorf("expired 5h = %+v", got.Windows[0])
	}
	if ok, errD := store.Delete(ctx, "opencode-go"); !ok || errD != nil {
		t.Fatalf("delete = %v %v", ok, errD)
	}
}
