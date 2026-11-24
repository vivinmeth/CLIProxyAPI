package auth

import (
	"context"
	"reflect"
	"sort"
	"testing"
	"time"
)

// The quota poller asks which over-threshold windows its poll at `at` left unrefreshed.
func TestTmxOverThresholdWindowsBefore(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	m := NewManager(nil, &ResetSoonestSelector{}, nil)
	if _, err := m.Register(context.Background(), &Auth{ID: "zx", Provider: "claude", Status: StatusActive}); err != nil {
		t.Fatal(err)
	}
	full, low := 98.0, 10.0
	m.ApplyQuotaObservation("zx", "claude", []QuotaWindowObservation{
		{Window: "7d", UsedPercent: &full, ResetsAt: now.Add(40 * time.Hour), ObservedAt: now.Add(-time.Hour)},
		{Window: "7d:fable-5", UsedPercent: &full, ResetsAt: now.Add(40 * time.Hour), ObservedAt: now.Add(-time.Hour)},
		{Window: "5h", UsedPercent: &low, ResetsAt: now.Add(2 * time.Hour), ObservedAt: now.Add(-time.Hour)},
	})
	if got := m.TmxOverThresholdWindowsBefore("zx", now); !reflect.DeepEqual(got, []string{"7d", "7d_oi"}) {
		t.Fatalf("missing = %v, want [7d 7d_oi]", got)
	}
	// A poll at now that carries 7d (still full) refreshes it: only the Fable window is missing.
	m.ApplyQuotaObservation("zx", "claude", []QuotaWindowObservation{
		{Window: "7d", UsedPercent: &full, ResetsAt: now.Add(40 * time.Hour), ObservedAt: now},
	})
	if got := m.TmxOverThresholdWindowsBefore("zx", now); !reflect.DeepEqual(got, []string{"7d_oi"}) {
		t.Fatalf("missing = %v, want [7d_oi]", got)
	}
	if got := m.TmxOverThresholdWindowsBefore("unknown", now); got != nil {
		t.Fatalf("unknown auth = %v", got)
	}
}

// The reader names only windows routing weighs for some model: 7d_oauth_apps and 7d_cowork never
// count, while the family and model-scoped windows do (the union over models).
func TestTmxOverThresholdWindowsBeforeOnlyRoutingWindows(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	m := NewManager(nil, &ResetSoonestSelector{}, nil)
	if _, err := m.Register(context.Background(), &Auth{ID: "zx", Provider: "claude", Status: StatusActive}); err != nil {
		t.Fatal(err)
	}
	full := 99.0
	var observations []QuotaWindowObservation
	for _, w := range []string{"7d_oauth_apps", "7d_cowork", "7d_opus", "7d:mythos-1"} {
		observations = append(observations, QuotaWindowObservation{Window: w, UsedPercent: &full, ResetsAt: now.Add(40 * time.Hour), ObservedAt: now.Add(-time.Hour)})
	}
	m.ApplyQuotaObservation("zx", "claude", observations)
	got := m.TmxOverThresholdWindowsBefore("zx", now)
	sort.Strings(got)
	if !reflect.DeepEqual(got, []string{"7d_model_mythos-1", "7d_opus"}) {
		t.Fatalf("missing = %v, want [7d_model_mythos-1 7d_opus]", got)
	}
}
