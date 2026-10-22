package requestlog

import (
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/muse"
)

// HarnessFor names the client that produced a row, so live traffic and imported sessions share
// labels. Imported rows ("extern:<harness>") keep their harness; proxied rows are classified by
// user agent. clientMode separates how a harness was run (interactive cli, print mode, sdk,
// exec, tui); it is "" when unknown.
func HarnessFor(source, userAgent string) (harness, clientMode string) {
	if h, ok := strings.CutPrefix(source, "extern:"); ok {
		return h, ""
	}
	ua := strings.ToLower(strings.TrimSpace(userAgent))
	name, _, _ := strings.Cut(ua, "/")
	switch {
	case ua == "":
		return "", ""
	case name == "claude-cli":
		switch {
		case strings.Contains(ua, "agent-sdk/"):
			return "claude-agent-sdk", "sdk"
		case strings.Contains(ua, "sdk-cli"):
			return "claude-code", "print"
		case strings.Contains(ua, "sdk-"):
			return "claude-agent-sdk", "sdk"
		default:
			return "claude-code", "cli"
		}
	case muse.IsClient(ua, ""):
		return muse.Harness, muse.ClientMode(ua, "")
	case name == "codex_exec":
		return "codex", "exec"
	case name == "codex-tui" || name == "codex_cli_rs" || name == "codex_vscode":
		return "codex", "tui"
	case strings.HasPrefix(name, "codex"):
		return "codex", ""
	case name == "opencode", name == "cursor", name == "aider", name == "nanx-app", name == "nanx-cli",
		name == "gemini-cli", name == "pi":
		return name, ""
	case name == "curl", name == "python-requests", strings.HasPrefix(name, "python-httpx"),
		strings.HasPrefix(name, "node-fetch"), name == "axios", name == "go-http-client":
		return "script", ""
	}
	return "other", ""
}
