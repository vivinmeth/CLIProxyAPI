package helps

import (
	"net/http"
	"testing"
	"time"
)

func TestParseClaudeOverloadRetryAfterHasNoQuotaFuzz(t *testing.T) {
	now := time.Now()
	headers := http.Header{
		"Retry-After": {"2"},
		// A quota reset on an overload response is not this response's recovery time.
		"Anthropic-Ratelimit-Unified-Status": {"rejected"},
		"Anthropic-Ratelimit-Unified-Reset":  {now.Add(3 * time.Hour).Format(time.RFC3339)},
	}
	for i := 0; i < 200; i++ {
		got := ParseClaudeOverloadRetryAfter(headers, now)
		if got == nil || *got < 2*time.Second || *got >= 3*time.Second {
			t.Fatalf("overload retry-after = %v, want [2s, 3s)", got)
		}
	}
}

func TestParseClaudeOverloadRetryAfterWithoutHint(t *testing.T) {
	now := time.Now()
	for _, headers := range []http.Header{nil, {}, {"Retry-After": {"0"}}, {"Retry-After": {"soon"}}, {"Retry-After": {"1", "2"}}} {
		if got := ParseClaudeOverloadRetryAfter(headers, now); got != nil {
			t.Fatalf("headers %v: retry-after = %v, want nil (overload default applies)", headers, *got)
		}
	}
}
