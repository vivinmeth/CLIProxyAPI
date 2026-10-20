package auth

import (
	"context"
	"testing"
	"time"
)

func TestManager_ClearQuotaSignalsDropsObservationsOnly(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	ctx := context.Background()
	observed := time.Now().Add(-time.Minute)
	next := time.Now().Add(time.Hour)
	signals := map[string]string{"Anthropic-Ratelimit-Unified-7d-Reset": "1793102375"}
	if _, err := manager.Register(ctx, &Auth{
		ID:       "tmx-signals",
		Provider: "claude",
		Quota:    QuotaState{Signals: signals, ObservedAt: observed, NextRecoverAt: next},
		ModelStates: map[string]*ModelState{
			"m": {Quota: QuotaState{Signals: signals, ObservedAt: observed}},
		},
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if !manager.ClearQuotaSignals("tmx-signals") {
		t.Fatal("expected signals to be cleared")
	}
	got, ok := manager.GetByID("tmx-signals")
	if !ok {
		t.Fatal("auth missing")
	}
	if len(got.Quota.Signals) != 0 || !got.Quota.ObservedAt.IsZero() {
		t.Fatalf("auth signals not cleared: %+v", got.Quota)
	}
	if !got.Quota.NextRecoverAt.Equal(next) {
		t.Fatal("cooldown fields must be untouched")
	}
	if ms := got.ModelStates["m"]; ms == nil || len(ms.Quota.Signals) != 0 {
		t.Fatalf("model signals not cleared: %+v", ms)
	}
	if manager.ClearQuotaSignals("tmx-signals") {
		t.Fatal("second clear must report no change")
	}
	if manager.ClearQuotaSignals("missing") {
		t.Fatal("unknown auth must report no change")
	}
}
