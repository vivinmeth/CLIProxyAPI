package helps

// tokenmaxx fork: a 529 (overloaded) or 503 is a seconds-long provider condition, not a quota
// window. Its explicit Retry-After is used as is, with at most claudeOverloadJitter added so
// parallel callers don't all wake at once, instead of the quota parser's 1–30 s grace period
// (which turned a 2 s overload hint into a 31 s client wait).

import (
	"math/rand/v2"
	"net/http"
	"time"
)

// claudeOverloadJitter bounds the random delay added to an overload Retry-After.
const claudeOverloadJitter = time.Second

// ParseClaudeOverloadRetryAfter returns the explicit Retry-After of an overloaded/unavailable
// response plus less than a second of jitter. Without a valid future Retry-After it returns nil,
// so the caller's short overload default applies.
func ParseClaudeOverloadRetryAfter(headers http.Header, now time.Time) *time.Duration {
	raw, ok := singleHeaderValueCaseInsensitive(headers, "Retry-After")
	if !ok {
		return nil
	}
	retryAt, ok := parseRetryAfterHeader(raw, now)
	if !ok || !retryAt.After(now) || retryAt.Sub(now) > maxClaudeRateLimitWindow {
		return nil
	}
	wait := retryAt.Sub(now) + rand.N(claudeOverloadJitter)
	return &wait
}
