package auth

import (
	"context"
	executor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	"net/http"
	"testing"
	"time"
)

func TestRedTeamPartialHeadersEraseWeeklyReset(t *testing.T) {
	now := resetSoonestTestNow
	a := observedAuth(t, "a", "claude", claudeWeeklyHeaders(now.Add(6*24*time.Hour), time.Time{}), now)
	b := observedAuth(t, "b", "claude", claudeWeeklyHeaders(now.Add(time.Hour), time.Time{}), now)
	a.Quota.ObserveResponseHeadersForProvider("claude", http.Header{"Anthropic-Ratelimit-Unified-5h-Utilization": {"0.1"}}, now.Add(time.Minute))
	got, err := newTestResetSoonestSelector(now.Add(time.Minute)).Pick(context.Background(), "claude", "", executor.Options{}, []*Auth{a, b})
	if err != nil || got.ID != "b" {
		t.Fatalf("partial 5h response reversed weekly order: %v %v", got, err)
	}
	if at := a.Quota.SignalObservedTime(claudeWeeklyResetSignal); !at.Equal(now) {
		t.Fatalf("partial response renewed weekly timestamp: %v", at)
	}
	if _, ok := weeklyResetAt(a, now.Add(QuotaObservationMaxAge+time.Minute)); ok {
		t.Fatal("stale weekly field still known")
	}
	if _, ok := weeklyResetAt(b, now.Add(2*time.Hour)); ok {
		t.Fatal("past weekly reset still known")
	}
}

func TestQuotaMergePreservesWindowClocksAndExpiresEvidence(t *testing.T) {
	now := resetSoonestTestNow
	var q QuotaState
	q.ObserveResponseHeadersForProvider("codex", http.Header{"X-Codex-Secondary-Reset-After-Seconds": {"7200"}, "X-Codex-Secondary-Window-Minutes": {"10080"}, "Retry-After": {"60"}, "X-Codex-Limit-Reached": {"true"}}, now)
	q.ObserveResponseHeadersForProvider("codex", http.Header{"X-Codex-Primary-Used-Percent": {"1"}}, now.Add(time.Minute))
	if q.Signals["Retry-After"] != "" || q.Signals["X-Codex-Limit-Reached"] != "" {
		t.Fatal("transient rejection retained")
	}
	a := &Auth{Quota: q}
	if reset, ok := weeklyResetAt(a, now.Add(time.Minute)); !ok || !reset.Equal(now.Add(2*time.Hour)) {
		t.Fatalf("relative reset clock moved: %v %v", reset, ok)
	}
	var rejection QuotaState
	rejection.ObserveResponseHeadersForProvider("codex", http.Header{"Retry-After": {"60"}}, now)
	if len(rejection.FreshSignals(now.Add(6*time.Minute))) != 0 {
		t.Fatal("rejection never expires without new traffic")
	}
	newer := q.Clone()
	newer.ObserveResponseHeadersForProvider("codex", http.Header{"X-Codex-Primary-Used-Percent": {"50"}}, now.Add(2*time.Minute))
	merged := mergeQuotaObservation(newer, q)
	if merged.Signals["X-Codex-Primary-Used-Percent"] != "50" {
		t.Fatal("older window overwrote fresh reading")
	}
	clone := q.Clone()
	clone.SignalObservedAt["X-Codex-Primary-Used-Percent"] = time.Time{}
	if q.SignalObservedTime("X-Codex-Primary-Used-Percent").IsZero() {
		t.Fatal("clone aliases timestamps")
	}
}
