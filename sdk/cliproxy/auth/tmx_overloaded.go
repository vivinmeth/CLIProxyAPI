package auth

// tokenmaxx fork: a 529 (Anthropic "overloaded") is a provider-wide, seconds-long condition, not
// a problem with the credential. It gets the same retry-round treatment as a 503 and a short
// cooldown instead of the generic 60 s one, so the pool is not drained by a brief overload.

import "time"

// overloadedCooldown is the cooldown after a 529 without a Retry-After hint.
const overloadedCooldown = 10 * time.Second

// overloadedFailureRetryAfter is recoverableFailureRetryAfterWithHint for a 529: the provider's
// hint wins, otherwise the shorter of overloadedCooldown and a configured transient cooldown.
func overloadedFailureRetryAfter(now time.Time, retryAfter *time.Duration, disableCooling bool) time.Time {
	if disableCooling {
		return time.Time{}
	}
	seconds := transientErrorCooldownSeconds.Load()
	if seconds < 0 {
		return time.Time{}
	}
	if retryAfter != nil && *retryAfter > 0 {
		return now.Add(*retryAfter)
	}
	cooldown := overloadedCooldown
	if configured := time.Duration(seconds) * time.Second; seconds > 0 && configured < cooldown {
		cooldown = configured
	}
	return now.Add(cooldown)
}
