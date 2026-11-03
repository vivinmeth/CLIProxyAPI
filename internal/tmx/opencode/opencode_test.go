package opencode

import (
	"math"
	"testing"
)

func TestNormalize(t *testing.T) {
	st := Settings{Entry: " OpenCode-Go ", Plan: "Go", Plans: map[string]map[string]float64{"Go": {"kimi-k3": 15}, "go-plus": {"kimi-k3": 60}}}
	if err := st.Normalize(); err != nil {
		t.Fatal(err)
	}
	if st.Entry != "opencode-go" || st.Plan != "go" || len(st.Windows) != 3 || st.Windows[0].Name != "5h" {
		t.Fatalf("normalized = %+v", st)
	}
	if c, ok := st.Cap("kimi-k3"); !ok || c != 15 {
		t.Fatalf("cap = %v %v", c, ok)
	}
	if _, ok := st.Cap("glm-5.3"); ok {
		t.Fatal("unknown model has a cap")
	}
	for name, bad := range map[string]Settings{
		"no entry":     {Plan: "go"},
		"no plan":      {Entry: "x"},
		"zero cap":     {Entry: "x", Plan: "go", Plans: map[string]map[string]float64{"go": {"m": 0}}},
		"nan cap":      {Entry: "x", Plan: "go", Plans: map[string]map[string]float64{"go": {"m": math.NaN()}}},
		"bad share":    {Entry: "x", Plan: "go", Windows: []Window{{"5h", 5, 1.5}}},
		"dup window":   {Entry: "x", Plan: "go", Windows: []Window{{"5h", 5, 0.2}, {"5h", 6, 0.2}}},
		"zero hours":   {Entry: "x", Plan: "go", Windows: []Window{{"5h", 0, 0.2}}},
		"empty plan k": {Entry: "x", Plan: "go", Plans: map[string]map[string]float64{" ": {"m": 1}}},
	} {
		if err := bad.Normalize(); err == nil {
			t.Errorf("%s: want error", name)
		}
	}
}

func TestReadingFor(t *testing.T) {
	st := Settings{Entry: "opencode-go", Plan: "go", Plans: map[string]map[string]float64{"go": {"kimi-k3": 15}}}
	if err := st.Normalize(); err != nil {
		t.Fatal(err)
	}
	// 5h cap = 3, 7d = 7.5, 30d = 15.
	r := readingFor(st, "kimi-k3", []float64{3, 6.5, 6.5})
	want := []struct {
		pct    float64
		status string
	}{{100, StatusAtLimit}, {86.6667, StatusWarning}, {43.3333, StatusAllowed}}
	for i, w := range want {
		got := r.Windows[i]
		if got.UsedPct == nil || math.Abs(*got.UsedPct-w.pct) > 0.001 || got.Status != w.status {
			t.Errorf("window %s = %v %s, want %.4f %s", got.Window, got.UsedPct, got.Status, w.pct, w.status)
		}
	}
	// A model with no cap under the plan reads unknown, never a guess.
	r = readingFor(st, "glm-5.3", []float64{1, 1, 1})
	for _, w := range r.Windows {
		if w.UsedPct != nil || w.CapUSD != nil || w.Status != StatusUnknownCap || w.UsedUSD != 1 {
			t.Errorf("unknown cap window = %+v", w)
		}
	}
}
