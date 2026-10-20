package requestlog

import "testing"

func TestHarnessFor(t *testing.T) {
	cases := []struct{ source, ua, harness, mode string }{
		{"tmx-proxy", "claude-cli/2.1.304 (external, sdk-ts, agent-sdk/0.3.276)", "claude-agent-sdk", "sdk"},
		{"tmx-proxy", "claude-cli/2.1.304 (external, sdk-cli, agent-sdk/0.3.276)", "claude-agent-sdk", "sdk"},
		{"tmx-proxy", "claude-cli/2.1.304 (external, sdk-cli)", "claude-code", "print"},
		{"tmx-proxy", "claude-cli/2.1.304 (external, cli)", "claude-code", "cli"},
		{"tmx-proxy", "codex_exec/0.163.0 (Ubuntu 24.4.0; x86_64) unknown (codex_exec; 0.163.0)", "codex", "exec"},
		{"tmx-proxy", "codex-tui/0.154.0 (Mac OS 26.5.2; arm64) iTerm.app/3.6.11", "codex", "tui"},
		{"tmx-proxy", "opencode/1.15.5", "opencode", ""},
		{"tmx-proxy", "curl/8.7.1", "script", ""},
		{"tmx-proxy", "SomethingElse/1", "other", ""},
		{"tmx-proxy", "", "", ""},
		{"extern:opencode", "opencode/1.2.3", "opencode", ""},
		{"extern:claude-code", "claude-code/2.1.304", "claude-code", ""},
	}
	for _, c := range cases {
		h, m := HarnessFor(c.source, c.ua)
		if h != c.harness || m != c.mode {
			t.Errorf("HarnessFor(%q, %q) = %q, %q; want %q, %q", c.source, c.ua, h, m, c.harness, c.mode)
		}
	}
}
