package auth

import (
	"context"
	"math"
	"strconv"
	"strings"
	"time"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

// weeklyWindowMinutes is the length of the long (weekly) subscription quota window.
const weeklyWindowMinutes = 7 * 24 * 60

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
// Ordering inside the highest available priority tier:
//  1. credentials with no observed weekly reset (so each one gets observed quickly),
//  2. credentials with a known future reset, earliest first,
//  3. credentials whose observed reset already passed (their new window is ~7 days away).
//
// Ties keep the stable fill-first (ID) order. Credentials without quota observation
// (API keys, Gemini, ...) therefore behave like fill-first.
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
	now := s.now()
	available, err := getSelectorAvailableAuths(ctx, auths, provider, model, now)
	if err != nil {
		return nil, err
	}
	available = preferCodexWebsocketAuths(ctx, provider, available)
	return pickResetSoonest(available, now), nil
}

const (
	resetSoonestRankUnknown = iota
	resetSoonestRankKnown
	resetSoonestRankPast
)

// resetSoonestKey returns the ordering rank and reset time for one credential.
func resetSoonestKey(auth *Auth, now time.Time) (int, time.Time) {
	resetAt, ok := weeklyResetAt(auth, now)
	if !ok {
		return resetSoonestRankUnknown, time.Time{}
	}
	if !resetAt.After(now) {
		return resetSoonestRankPast, time.Time{}
	}
	return resetSoonestRankKnown, resetAt
}

// pickResetSoonest returns the first auth in reset-soonest order, keeping the input order
// for ties. The input is expected to be ID-sorted (fill-first order).
func pickResetSoonest(auths []*Auth, now time.Time) *Auth {
	var picked *Auth
	pickedRank := 0
	var pickedReset time.Time
	for _, candidate := range auths {
		if candidate == nil {
			continue
		}
		rank, resetAt := resetSoonestKey(candidate, now)
		if picked == nil || rank < pickedRank || (rank == pickedRank && rank == resetSoonestRankKnown && resetAt.Before(pickedReset)) {
			picked = candidate
			pickedRank = rank
			pickedReset = resetAt
		}
	}
	return picked
}

// weeklyResetAt extracts the observed reset time of the credential's weekly quota window
// from its passive quota signals. It reports false when no weekly reset was observed.
//
// Claude: the earliest of the shared 7-day window and the model-specific 7-day window
// (7d_oi). Codex: the secondary window, falling back to the primary window only when it
// is itself a weekly window. Devin: weekly_quota_reset_at.
//
// When a credential carries several weekly windows, the earliest reset still in the
// future wins; a reset that already passed only counts when no future reset remains,
// because that window has rolled over and its next reset is about a week away.
func weeklyResetAt(auth *Auth, now time.Time) (time.Time, bool) {
	if auth == nil || len(auth.Quota.Signals) == 0 {
		return time.Time{}, false
	}
	signals := auth.Quota.Signals
	observedAt := auth.Quota.ObservedAt

	var candidates []time.Time
	for _, name := range []string{claudeWeeklyResetSignal, claudeWeeklyModelResetSignal} {
		if resetAt, ok := parseQuotaResetTimestamp(quotaSignalValue(signals, name)); ok {
			candidates = append(candidates, resetAt)
		}
	}
	if resetAt, ok := codexWindowResetAt(signals, observedAt, codexSecondaryResetAtSignal, codexSecondaryResetAfterSignal); ok {
		candidates = append(candidates, resetAt)
	} else if minutes, okMinutes := parseQuotaInt(quotaSignalValue(signals, codexPrimaryWindowSignal)); okMinutes && minutes >= weeklyWindowMinutes {
		if resetAtPrimary, okPrimary := codexWindowResetAt(signals, observedAt, codexPrimaryResetAtSignal, codexPrimaryResetAfterSignal); okPrimary {
			candidates = append(candidates, resetAtPrimary)
		}
	}
	if resetAt, ok := parseQuotaResetTimestamp(quotaSignalValue(signals, devinWeeklyResetSignal)); ok {
		candidates = append(candidates, resetAt)
	}
	if len(candidates) == 0 {
		return time.Time{}, false
	}

	var earliestFuture, latestPast time.Time
	for _, candidate := range candidates {
		if candidate.After(now) {
			if earliestFuture.IsZero() || candidate.Before(earliestFuture) {
				earliestFuture = candidate
			}
			continue
		}
		if latestPast.IsZero() || candidate.After(latestPast) {
			latestPast = candidate
		}
	}
	if !earliestFuture.IsZero() {
		return earliestFuture, true
	}
	return latestPast, true
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
