package codexturn

import (
	"net/http"
	"strings"
)

// TurnRateLimitCode is the ChatGPT backend's account-wide "Codex is at capacity" 429.
const TurnRateLimitCode = "turn_rate_limit_exceeded"

// IsTurnRateLimit reports whether an upstream failure is Codex's turn rate limit.
//
// Upstream closes a Responses WebSocket silently on 429s so the client reconnects and lands on
// another credential. For this 429 that backfires: every reconnect starts a new turn on the
// throttled account, so Codex spends its retries in seconds and fails. Exposing the error frame
// (with Retry-After) lets Codex wait as long as the backend asks, as it does on a direct login.
func IsTurnRateLimit(status int, err error) bool {
	if status != http.StatusTooManyRequests || err == nil {
		return false
	}
	return strings.Contains(err.Error(), TurnRateLimitCode)
}
