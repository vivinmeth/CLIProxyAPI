package executor

import (
	"strings"
	"testing"
)

// tmxSkipCodexToClaude skips upstream test cases that send a Codex (Responses API) request to a
// Claude provider. The tokenmaxx fork refuses those by owner rule: no Claude model reaches a Codex
// client (internal/tmx/clients, sdk/api/handlers/handlers_routing_tmx.go).
func tmxSkipCodexToClaude(t *testing.T, provider string) {
	t.Helper()
	if provider == "" || strings.HasPrefix(strings.ToLower(provider), "claude") {
		t.Skip("tokenmaxx: Claude models are not served to Codex / Responses API clients")
	}
}
