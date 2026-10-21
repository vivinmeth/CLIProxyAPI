package auth

import (
	"strconv"
	"testing"
	"time"
)

func TestResetSoonestStaleExhaustedAccountIsNotProbedFirst(t *testing.T) {
	now := time.Now()
	weekly := func(id string, used string, reset, observed time.Time) *Auth {
		return &Auth{ID: id, Provider: "claude", Status: StatusActive, Quota: QuotaState{ObservedAt: observed, Signals: map[string]string{
			"Anthropic-Ratelimit-Unified-7d-Utilization": used,
			"Anthropic-Ratelimit-Unified-7d-Reset":       strconv.FormatInt(reset.Unix(), 10),
		}}}
	}
	// Last seen at 99% of a week that hasn't reset, but the reading is past its TTL.
	stale := weekly("stale-exhausted", "0.99", now.Add(72*time.Hour), now.Add(-QuotaObservationMaxAge-time.Hour))
	healthy := weekly("healthy", "0.3", now.Add(120*time.Hour), now.Add(-time.Minute))
	if got := pickResetSoonestForModel([]*Auth{stale, healthy}, now, "claude-sonnet-5", admissionView(nil)); got != healthy {
		t.Fatalf("picked %s; a stale but last-known exhausted account must stay behind headroom", got.ID)
	}
	// Alone, it may still be probed for a fresh reading.
	if got := pickResetSoonestForModel([]*Auth{stale}, now, "claude-sonnet-5", admissionView(nil)); got != stale {
		t.Fatal("the stale account must remain a last resort")
	}
	// Once its week rolled over, the old utilization no longer applies.
	rolled := weekly("rolled", "0.99", now.Add(-time.Hour), now.Add(-8*24*time.Hour))
	if overQuotaThreshold(rolled, now, "claude-sonnet-5", admissionView(nil)) {
		t.Fatal("utilization of a past window must not penalize")
	}
}
