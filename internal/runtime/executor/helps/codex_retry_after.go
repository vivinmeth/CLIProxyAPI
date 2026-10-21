package helps

import (
	"net/http"
	"strconv"
	"strings"
	"time"
)

// maxCodexRetryHint bounds header-supplied Codex deadlines. Codex windows run up to a month;
// anything later is not a recovery time.
const maxCodexRetryHint = 31 * 24 * time.Hour

// ParseRetryAfterDeadline parses a standard Retry-After value (delta-seconds or HTTP-date) into
// an absolute deadline. It reports false for a missing, malformed, past, or implausibly distant
// value.
func ParseRetryAfterDeadline(headers http.Header, now time.Time) (time.Time, bool) {
	raw := getHeaderCaseInsensitive(headers, "Retry-After")
	if raw == "" {
		return time.Time{}, false
	}
	deadline, ok := parseRetryAfterHeader(raw, now)
	if !ok || !deadline.After(now) || deadline.Sub(now) > maxCodexRetryHint {
		return time.Time{}, false
	}
	return deadline, true
}

// CodexExhaustedWindowReset returns the latest reset among the Codex rate-limit windows that the
// response headers report as exhausted (X-Codex-Primary-* / X-Codex-Secondary-*, used percent at
// or above 100). Windows with room left say nothing about when an exhausted account recovers.
func CodexExhaustedWindowReset(headers http.Header, now time.Time) (time.Time, bool) {
	var latest time.Time
	for _, slot := range []string{"X-Codex-Primary-", "X-Codex-Secondary-"} {
		used, errUsed := strconv.ParseFloat(strings.TrimSpace(getHeaderCaseInsensitive(headers, slot+"Used-Percent")), 64)
		if errUsed != nil || used < 100 {
			continue
		}
		var reset time.Time
		if at, errAt := strconv.ParseInt(strings.TrimSpace(getHeaderCaseInsensitive(headers, slot+"Reset-At")), 10, 64); errAt == nil && at > 0 {
			reset = time.Unix(at, 0)
		} else if after, errAfter := strconv.ParseInt(strings.TrimSpace(getHeaderCaseInsensitive(headers, slot+"Reset-After-Seconds")), 10, 64); errAfter == nil && after > 0 {
			reset = now.Add(time.Duration(after) * time.Second)
		}
		if !reset.After(now) || reset.Sub(now) > maxCodexRetryHint {
			continue
		}
		if reset.After(latest) {
			latest = reset
		}
	}
	return latest, !latest.IsZero()
}

// CodexErrorResponseHeaders keeps the headers of a Codex error response that describe its retry
// timing and quota windows; everything else (cookies, tracing, turn state) is dropped.
func CodexErrorResponseHeaders(headers http.Header) http.Header {
	var kept http.Header
	for key, values := range headers {
		lower := strings.ToLower(key)
		keep := lower == "retry-after" ||
			lower == "x-codex-plan-type" || lower == "x-codex-active-limit" ||
			lower == "x-codex-allowed" || lower == "x-codex-limit-reached" ||
			strings.HasPrefix(lower, "x-codex-primary-") ||
			strings.HasPrefix(lower, "x-codex-secondary-") ||
			strings.HasPrefix(lower, "x-codex-credits-")
		if !keep || len(values) == 0 {
			continue
		}
		if kept == nil {
			kept = make(http.Header)
		}
		kept[http.CanonicalHeaderKey(key)] = append([]string(nil), values...)
	}
	return kept
}
