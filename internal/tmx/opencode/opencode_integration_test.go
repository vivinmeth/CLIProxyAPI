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
	read := func(st opencode.Settings, at time.Time) (opencode.Reading, error) {
		rs, err := store.ReadAll(ctx, st, at)
		if err != nil || len(rs) != 1 {
			return opencode.Reading{}, fmt.Errorf("readings %d: %v", len(rs), err)
		}
		return rs[0], nil
	}
	got, err := read(st, now)
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
	if got, _ = read(st, now); got.PoolSource != opencode.PoolFromTraffic || got.PoolUSD == nil || *got.PoolUSD != 60 {
		t.Errorf("go-plus pool = %v %s", got.PoolUSD, got.PoolSource)
	}
	// An unknown plan reads unknown, never a guess.
	st.Plan = "go-max"
	if got, _ = read(st, now); got.PoolUSD != nil || got.Windows[0].Status != opencode.StatusUnknownCap || got.Windows[0].UsedPct != nil {
		t.Errorf("unknown plan = %+v", got)
	}
	// Once the open window has closed, the 5h reading is zero with no reset time.
	st.Plan = "go"
	if got, _ = read(st, now.Add(3*time.Hour)); got.Windows[0].UsedUSD != 0 || got.Windows[0].ResetsAt != nil {
		t.Errorf("expired 5h = %+v", got.Windows[0])
	}
	if ok, errD := store.Delete(ctx, "opencode-go"); !ok || errD != nil {
		t.Fatalf("delete = %v %v", ok, errD)
	}
}

// Several accounts in one entry's pool: each read against its own plan from its own keys' rows;
// a rotated key's old fingerprint keeps the history; a key no account lists reads as unassigned.
func TestReadAllAccounts(t *testing.T) {
	dsn := os.Getenv("TMX_REQUESTLOG_TEST_DSN")
	if dsn == "" {
		t.Skip("TMX_REQUESTLOG_TEST_DSN not set")
	}
	ctx := context.Background()
	schema := fmt.Sprintf("tmx_test_oca_%d", time.Now().UnixNano())
	rl, err := requestlog.Open(ctx, dsn, schema, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = rl.DB().Exec("DROP SCHEMA " + schema + " CASCADE")
		rl.Close()
	})
	now := time.Now().UTC().Truncate(time.Second)
	keyA, keyAOld, keyB, keyC := requestlog.KeyFingerprint("oc_sk_a"), requestlog.KeyFingerprint("oc_sk_a_old"),
		requestlog.KeyFingerprint("oc_sk_b"), requestlog.KeyFingerprint("oc_sk_c")
	for _, x := range []struct {
		ago                time.Duration
		label, model       string
		allowance, listUSD float64
	}{
		{4 * time.Hour, keyAOld, "kimi-k3", 6, 1.5},     // before A's key was rotated: same 5h window
		{time.Hour, keyA, "kimi-k3", 6.5, 1.625},        // A: 12.5 of Go's 12 -> at limit
		{2 * time.Hour, keyB, "kimi-k3", 3, 3},          // B on Go Plus: allowance = list here
		{30 * time.Minute, keyC, "glm-5.3-flash", 1, 1}, // a key no account lists
	} {
		if _, err = rl.DB().ExecContext(ctx, `INSERT INTO `+schema+`.requests (ts, provider, model, outcome, auth_label,
			allowance_usd, cost_usd, price_source) VALUES ($1, 'openai-compatible-opencode-go', $2, 'success', $3, $4, $5, 'opencode')`,
			now.Add(-x.ago), x.model, x.label, x.allowance, x.listUSD); err != nil {
			t.Fatal(err)
		}
	}
	store, err := opencode.NewStore(ctx, rl.DB(), schema)
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Put(ctx, opencode.Settings{Entry: "opencode-go", Plan: "go",
		Plans: map[string]opencode.Plan{
			"go":      {Pool: 60, Caps: map[string]float64{"kimi-k3": 15, "glm-5.3-flash": 60}},
			"go-plus": {Caps: map[string]float64{"kimi-k3": 60}}, // no pool: from B's rows, 60 x 3 / 3 = 60
		},
		Accounts: []opencode.Account{
			{Name: "account-1", Keys: []string{keyA, keyAOld}},
			{Name: "account-2", Plan: "go-plus", Keys: []string{keyB}},
		}})
	if err != nil {
		t.Fatal(err)
	}
	rs, err := store.ReadAll(ctx, st, now)
	if err != nil {
		t.Fatal(err)
	}
	by := map[string]opencode.Reading{}
	for _, r := range rs {
		by[r.Account] = r
	}
	if len(rs) != 3 || by["account-1"].Plan != "go" || by["account-2"].Plan != "go-plus" {
		t.Fatalf("readings = %+v", rs)
	}
	a5 := by["account-1"].Windows[0]
	if a5.UsedUSD != 12.5 || a5.Status != opencode.StatusAtLimit || a5.ResetsAt == nil || !a5.ResetsAt.Equal(now.Add(time.Hour)) {
		t.Errorf("account-1 5h = %+v (resets %v)", a5, a5.ResetsAt)
	}
	b5 := by["account-2"].Windows[0]
	if b5.UsedUSD != 3 || by["account-2"].PoolSource != opencode.PoolFromTraffic || *by["account-2"].PoolUSD != 60 || b5.Status != opencode.StatusAllowed {
		t.Errorf("account-2 = %+v / 5h %+v", by["account-2"], b5)
	}
	if u := by[opencode.Unassigned]; u.Windows[0].UsedUSD != 1 || u.Plan != "go" {
		t.Errorf("unassigned = %+v", u)
	}
	// A key in two accounts, or a non-fingerprint, is refused.
	for _, bad := range [][]opencode.Account{
		{{Name: "x", Keys: []string{keyA}}, {Name: "y", Keys: []string{keyA}}},
		{{Name: "x", Keys: []string{"oc_sk_raw_key"}}},
		{{Name: "unassigned", Keys: []string{keyA}}},
	} {
		if _, err := store.Put(ctx, opencode.Settings{Entry: "e", Plan: "go", Accounts: bad}); err == nil {
			t.Errorf("accounts %+v accepted", bad)
		}
	}
}
