package opencode

import (
	"math"
	"testing"
)

func TestNormalize(t *testing.T) {
	st := Settings{Entry: " OpenCode-Go ", Plan: "Go", Plans: map[string]Plan{
		"Go": {Pool: 60, Caps: map[string]float64{"kimi-k3": 15}}, "go-plus": {Caps: map[string]float64{"kimi-k3": 60}}}}
	if err := st.Normalize(); err != nil {
		t.Fatal(err)
	}
	if st.Entry != "opencode-go" || st.Plan != "go" || len(st.Windows) != 3 || !st.Windows[0].Anchored || st.Windows[1].Anchored {
		t.Fatalf("normalized = %+v", st)
	}
	if st.Plans["go"].Pool != 60 || st.Plans["go-plus"].Pool != 0 || st.Plans["go"].Caps["kimi-k3"] != 15 {
		t.Fatalf("plans = %+v", st.Plans)
	}
	for name, bad := range map[string]Settings{
		"no entry":     {Plan: "go"},
		"no plan":      {Entry: "x"},
		"zero limit":   {Entry: "x", Plan: "go", Plans: map[string]Plan{"go": {Caps: map[string]float64{"m": 0}}}},
		"nan limit":    {Entry: "x", Plan: "go", Plans: map[string]Plan{"go": {Caps: map[string]float64{"m": math.NaN()}}}},
		"neg pool":     {Entry: "x", Plan: "go", Plans: map[string]Plan{"go": {Pool: -1}}},
		"inf pool":     {Entry: "x", Plan: "go", Plans: map[string]Plan{"go": {Pool: math.Inf(1)}}},
		"bad share":    {Entry: "x", Plan: "go", Windows: []Window{{"5h", 5, 1.5, true}}},
		"dup window":   {Entry: "x", Plan: "go", Windows: []Window{{"5h", 5, 0.2, true}, {"5h", 6, 0.2, false}}},
		"zero hours":   {Entry: "x", Plan: "go", Windows: []Window{{"5h", 0, 0.2, true}}},
		"empty plan k": {Entry: "x", Plan: "go", Plans: map[string]Plan{" ": {Pool: 1}}},
	} {
		if err := bad.Normalize(); err == nil {
			t.Errorf("%s: want error", name)
		}
	}
}

func TestPoolFor(t *testing.T) {
	caps := map[string]float64{"kimi-k3": 15, "glm-5.3-flash": 60, "qwen3.8-flash": 30}
	// Observed on Go, 2026-11-03: allowance = list x 60 / limit.
	models := []ModelUse{
		{Model: "kimi-k3", AllowanceUSD: 4.923972, ListCostUSD: 1.230993},
		{Model: "glm-5.3-flash", AllowanceUSD: 0.0000042, ListCostUSD: 0.0000042},
		{Model: "qwen3.8-flash", AllowanceUSD: 0.00468756, ListCostUSD: 0.00234378},
		{Model: "no-limit-known", AllowanceUSD: 9, ListCostUSD: 1},
	}
	set := Settings{Plan: "go", Plans: map[string]Plan{"go": {Pool: 60, Caps: caps}, "go-plus": {Caps: caps}}}
	if p, src := poolFor(set, models); p != 60 || src != PoolFromSettings {
		t.Fatalf("settings pool = %v %s", p, src)
	}
	set.Plan = "go-plus"
	if p, src := poolFor(set, models); p != 60 || src != PoolFromTraffic {
		t.Fatalf("traffic pool = %v %s", p, src)
	}
	if _, src := poolFor(set, nil); src != "" {
		t.Fatal("pool from no traffic")
	}
	set.Plan = "go-max"
	if _, src := poolFor(set, models); src != "" {
		t.Fatal("pool for an unknown plan")
	}
}

func TestApplyCap(t *testing.T) {
	pool := 60.0
	for _, tc := range []struct {
		used, share float64
		pct         float64
		status      string
	}{{14.79, 0.2, 123.25, StatusAtLimit}, {10, 0.2, 83.3333, StatusWarning}, {3, 0.5, 10, StatusAllowed}} {
		wr := WindowReading{UsedUSD: tc.used}
		applyCap(&wr, &pool, tc.share)
		if wr.UsedPct == nil || math.Abs(*wr.UsedPct-tc.pct) > 0.001 || wr.Status != tc.status || *wr.CapUSD != pool*tc.share {
			t.Errorf("%v of %v: %+v", tc.used, tc.share, wr)
		}
	}
	wr := WindowReading{UsedUSD: 1}
	applyCap(&wr, nil, 0.2)
	if wr.UsedPct != nil || wr.CapUSD != nil || wr.Status != StatusUnknownCap {
		t.Errorf("unknown pool = %+v", wr)
	}
}
