package quota

import "testing"

// The pollers call Anthropic as Claude Code; a stale copy of the user agent
// drifted to 2.1.280 while the executor moved on (2026-11-10).
func TestClaudeCLIUserAgentFollowsExecutorBaseline(t *testing.T) {
	if want := "claude-cli/2.1.321 (external, cli)"; ClaudeCLIUserAgent != want {
		t.Fatalf("ClaudeCLIUserAgent = %q, want %q", ClaudeCLIUserAgent, want)
	}
	if got := claudeHeaders["User-Agent"]; got != ClaudeCLIUserAgent {
		t.Fatalf("claudeHeaders User-Agent = %q, want %q", got, ClaudeCLIUserAgent)
	}
}
