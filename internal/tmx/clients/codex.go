// Package clients holds the tokenmaxx fork's per-client model rules. The owner's rule: no Claude
// model ever reaches a Codex client, by model list, alias or a request by name. Claude work goes
// through Claude Code.
package clients

import (
	"encoding/json"
	"net/http"
	"regexp"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/muse"
)

// codexUserAgent matches the user agent every Codex build sends, whatever originator it was given:
//
//	codex_exec/0.165.0 (Ubuntu 24.4.0; x86_64) dumb (codex_exec; 0.165.0)
//	T3 Code/0.166.0 (Mac OS 26.2.0; arm64) unknown (T3 Code; 0.1.7-nightly.20261102.4066)
//
// <originator>/<semver> (<os>; <arch>) <terminal>, with an optional "(<originator>; <version>)" tail.
var codexUserAgent = regexp.MustCompile(`^[^/()]+/\d+\.\d+\.\d+\S* \([^()]*; [^()]*\) \S+( \([^()]*\))?$`)

// codexNames are user-agent product names (before the "/") that only Codex sends.
var codexNames = []string{"codex_exec", "codex_cli_rs", "codex-tui", "codex_vscode", "codex desktop"}

// IsCodex reports whether a request comes from a Codex client: a known Codex product name, the
// Codex user-agent shape (T3's Codex, which keeps the shape under its own originator), or an
// Originator header (sent by Codex only). Claude Code and the Claude Agent SDK ("claude-cli/…") and
// Muse never count, so a misread can't refuse their own models.
func IsCodex(h http.Header) bool {
	if h == nil {
		return false
	}
	ua := strings.TrimSpace(h.Get("User-Agent"))
	lower := strings.ToLower(ua)
	if strings.HasPrefix(lower, "claude-cli") || muse.IsClientHeaders(h) {
		return false
	}
	name, _, _ := strings.Cut(lower, "/")
	for _, n := range codexNames {
		if name == n {
			return true
		}
	}
	if codexUserAgent.MatchString(ua) {
		return true
	}
	return strings.TrimSpace(h.Get("Originator")) != ""
}

// IsClaudeModel reports whether a model id names a Claude model, or any of the providers that serve
// it is Claude. Ids are compared case-insensitively and anywhere in the id, so routed names such as
// "anthropic/claude-sonnet-5" or an OpenCode "opencode-zen/claude-…" alias count too.
func IsClaudeModel(id string, providers []string) bool {
	if strings.Contains(strings.ToLower(id), "claude") {
		return true
	}
	for _, p := range providers {
		switch strings.ToLower(strings.TrimSpace(p)) {
		case "claude", "anthropic":
			return true
		}
	}
	return false
}

// Claude models refused to Codex clients answer with this error code.
const CodexClaudeErrorCode = "claude_models_not_for_codex"

// CodexClaudeErrorBody is the OpenAI-style 400 body for a Claude model requested by Codex.
func CodexClaudeErrorBody(model string) string {
	body, _ := json.Marshal(map[string]any{"error": map[string]any{
		"code":    CodexClaudeErrorCode,
		"message": "Claude models are not served to Codex clients on this proxy (" + model + "); use Claude Code for Claude work",
		"type":    "invalid_request_error",
		"param":   "model",
	}})
	return string(body)
}

// FilterModelsForCodex drops Claude models from a model list built for a Codex client. Each entry's
// "id" (or Codex catalog "slug") and "owned_by" are checked; providersOf, when set, returns a
// model's registered providers.
func FilterModelsForCodex(models []map[string]any, providersOf func(string) []string) []map[string]any {
	out := make([]map[string]any, 0, len(models))
	for _, m := range models {
		id, _ := m["id"].(string)
		if id == "" {
			id, _ = m["slug"].(string)
		}
		owner, _ := m["owned_by"].(string)
		var providers []string
		if providersOf != nil && id != "" {
			providers = providersOf(id)
		}
		if IsClaudeModel(id, append(providers, owner)) {
			continue
		}
		out = append(out, m)
	}
	return out
}
