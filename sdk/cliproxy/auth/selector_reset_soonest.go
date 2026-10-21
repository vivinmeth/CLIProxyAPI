package auth

import (
	"context"
	"math"
	"strconv"
	"strings"
	"time"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

// Accept provider weekly windows of at least six days.
const minimumWeeklyWindowMinutes = 6 * 24 * 60

// Quota signal names (canonical header keys as stored by collectQuotaSignals) that carry
// the reset time of a credential's weekly quota window.
const (
	// claudeWeeklyResetSignal is the shared Anthropic 7-day unified window reset (unix seconds).
	claudeWeeklyResetSignal = "Anthropic-Ratelimit-Unified-7d-Reset"
	// claudeWeeklyModelResetSignal is the model-specific (Fable, "oi") Anthropic 7-day window reset.
	claudeWeeklyModelResetSignal = "Anthropic-Ratelimit-Unified-7d_oi-Reset"
	// codexSecondaryResetAtSignal is the Codex secondary (weekly) window reset (unix seconds).
	codexSecondaryResetAtSignal = "X-Codex-Secondary-Reset-At"
	// codexSecondaryResetAfterSignal is the Codex secondary window reset relative to ObservedAt.
	codexSecondaryResetAfterSignal = "X-Codex-Secondary-Reset-After-Seconds"
	codexPrimaryWindowSignal       = "X-Codex-Primary-Window-Minutes"
	codexPrimaryResetAtSignal      = "X-Codex-Primary-Reset-At"
	codexPrimaryResetAfterSignal   = "X-Codex-Primary-Reset-After-Seconds"
	// devinWeeklyResetSignal is recorded by the Devin executor from its status endpoint (RFC3339).
	devinWeeklyResetSignal = "weekly_quota_reset_at"
)

// ResetSoonestSelector picks the available credential whose weekly quota window resets
// soonest, so quota that would otherwise expire unused at the reset is consumed first and
// credentials with distant resets are kept in reserve.
//
// Within the highest priority tier, prefer credentials below utilization thresholds,
// then accounts with admission headroom. Among healthy candidates, one unobserved
// probe leads, followed by earliest future weekly resets, then rolled-over readings.
// Saturated accounts and accounts near quota exhaustion remain usable as a last resort.
// Ties keep stable ID order; excess traffic above all soft caps uses in-flight counts.
type ResetSoonestSelector struct {
	// nowFunc overrides the clock for tests; nil uses time.Now.
	nowFunc func() time.Time
}

func (s *ResetSoonestSelector) now() time.Time {
	if s != nil && s.nowFunc != nil {
		return s.nowFunc()
	}
	return time.Now()
}

// Pick selects the available auth whose weekly quota window resets soonest.
func (s *ResetSoonestSelector) Pick(ctx context.Context, provider, model string, opts cliproxyexecutor.Options, auths []*Auth) (*Auth, error) {
	_ = opts
	if model == "" && ctx != nil {
		model, _ = ctx.Value(tmxRankingModelKey{}).(string)
	}
	now := s.now()
	available, err := getSelectorAvailableAuths(ctx, auths, provider, model, now)
	if err != nil {
		return nil, err
	}
	available = preferCodexWebsocketAuths(ctx, provider, available)
	return pickResetSoonestForModel(available, now, model, admissionView(ctx)), nil
}

const (
	resetSoonestRankUnknown = iota
	resetSoonestRankKnown
	resetSoonestRankPast
)

func resetSoonestKey(auth *Auth, now time.Time) (int, time.Time) {
	return rankingKey(auth, now, "")
}

func pickResetSoonest(auths []*Auth, now time.Time) *Auth {
	return pickResetSoonestForModel(auths, now, "", admissionView(nil))
}

// weeklyResetAt retains the original model-neutral helper for callers inspecting
// Claude's shared/Fable windows. Routing uses weeklyResetAtForModel instead.
// Only fresh observations with future resets are considered known.
func weeklyResetAt(auth *Auth, now time.Time) (time.Time, bool) {
	return weeklyResetAtForModel(auth, now, "fable")
}

func weeklyResetAtForModel(auth *Auth, now time.Time, model string) (time.Time, bool) {
	if auth == nil || len(auth.Quota.Signals) == 0 {
		return time.Time{}, false
	}
	signals := auth.Quota.FreshSignals(now)

	var candidates []time.Time
	weeklyNames := claudeWeeklyResetNames(signals, model)
	for _, name := range weeklyNames {
		if resetAt, ok := parseQuotaResetTimestamp(quotaSignalValue(signals, name)); ok {
			candidates = append(candidates, resetAt)
		}
	}
	if resetAt, ok := codexWindowResetAt(signals, auth.Quota.SignalObservedTime(codexSecondaryResetAfterSignal), codexSecondaryResetAtSignal, codexSecondaryResetAfterSignal); ok && codexWeeklyWindow(signals, "Secondary") {
		candidates = append(candidates, resetAt)
	} else if minutes, okMinutes := parseQuotaInt(quotaSignalValue(signals, codexPrimaryWindowSignal)); okMinutes && minutes >= minimumWeeklyWindowMinutes {
		if resetAtPrimary, okPrimary := codexWindowResetAt(signals, auth.Quota.SignalObservedTime(codexPrimaryResetAfterSignal), codexPrimaryResetAtSignal, codexPrimaryResetAfterSignal); okPrimary {
			candidates = append(candidates, resetAtPrimary)
		}
	}
	if resetAt, ok := parseQuotaResetTimestamp(quotaSignalValue(signals, devinWeeklyResetSignal)); ok {
		candidates = append(candidates, resetAt)
	}
	if len(candidates) == 0 {
		return time.Time{}, false
	}

	var earliestFuture time.Time
	for _, candidate := range candidates {
		if candidate.After(now) {
			if earliestFuture.IsZero() || candidate.Before(earliestFuture) {
				earliestFuture = candidate
			}
			continue
		}
	}
	if !earliestFuture.IsZero() {
		return earliestFuture, true
	}
	return time.Time{}, false
}

// codexWindowResetAt resolves a Codex window reset from its absolute reset-at signal or,
// when absent, from the reset-after-seconds signal relative to the observation time.
func codexWindowResetAt(signals map[string]string, observedAt time.Time, resetAtName, resetAfterName string) (time.Time, bool) {
	if resetAt, ok := parseQuotaResetTimestamp(quotaSignalValue(signals, resetAtName)); ok {
		return resetAt, true
	}
	if observedAt.IsZero() {
		return time.Time{}, false
	}
	seconds, ok := parseQuotaInt(quotaSignalValue(signals, resetAfterName))
	if !ok || seconds < 0 {
		return time.Time{}, false
	}
	return observedAt.Add(time.Duration(seconds) * time.Second), true
}

// quotaSignalValue looks up a signal by exact name first, then case-insensitively.
func quotaSignalValue(signals map[string]string, name string) string {
	if value, ok := signals[name]; ok {
		return strings.TrimSpace(value)
	}
	for key, value := range signals {
		if strings.EqualFold(key, name) {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func parseQuotaInt(raw string) (int64, bool) {
	if raw == "" {
		return 0, false
	}
	value, errParse := strconv.ParseFloat(raw, 64)
	if errParse != nil || math.IsNaN(value) || math.IsInf(value, 0) {
		return 0, false
	}
	return int64(value), true
}

// parseQuotaResetTimestamp parses a unix timestamp (seconds, or milliseconds when the
// magnitude requires it) or an RFC3339 timestamp.
func parseQuotaResetTimestamp(raw string) (time.Time, bool) {
	if raw == "" {
		return time.Time{}, false
	}
	if value, errParse := strconv.ParseFloat(raw, 64); errParse == nil {
		if value <= 0 || math.IsNaN(value) || math.IsInf(value, 0) {
			return time.Time{}, false
		}
		if value >= 1e12 {
			return time.UnixMilli(int64(value)), true
		}
		seconds := int64(value)
		return time.Unix(seconds, int64((value-float64(seconds))*1e9)), true
	}
	if parsed, errParse := time.Parse(time.RFC3339, raw); errParse == nil {
		return parsed, true
	}
	return time.Time{}, false
}
