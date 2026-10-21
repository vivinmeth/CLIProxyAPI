package auth

// tokenmaxx fork: the first 429 a client receives carries Retry-After. Before this only a
// cached-cooldown refusal (modelCooldownError, Home errors) had one; a refusal straight from the
// provider reached the client without a hint even though the executor had parsed its deadline.

import (
	"net/http"
	"time"
)

// statusOverloaded is Anthropic's non-standard "overloaded" status.
const statusOverloaded = 529

// maxClientRetryAfter caps the hint given to clients. A weekly quota reset days away is still
// reported, as "retry in a day", so a client never sleeps for most of a week on one answer.
const maxClientRetryAfter = 24 * time.Hour

// upstreamRetryAfterHeader returns Retry-After for an upstream refusal whose status allows the
// header (429, 503, 529) and whose executor parsed a deadline. Unknown deadlines get no header.
func upstreamRetryAfterHeader(err error) http.Header {
	switch statusCodeFromError(err) {
	case http.StatusTooManyRequests, http.StatusServiceUnavailable, statusOverloaded:
	default:
		return nil
	}
	retryAfter := retryAfterFromError(err)
	if retryAfter == nil || *retryAfter <= 0 {
		return nil
	}
	return safeRetryAfterHeader(min(*retryAfter, maxClientRetryAfter))
}
