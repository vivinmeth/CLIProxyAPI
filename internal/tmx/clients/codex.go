// Package clients holds the tokenmaxx fork's per-client model rules. The owner's rule: no Claude
// model ever reaches a Codex client, by model list, alias or a request by name. Claude work goes
// through Claude Code.
package clients

import (
	"encoding/json"
	"net/http"
	"regexp"
	"strings"
	"sync"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
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
	if strings.HasPrefix(lower, "claude-cli") || isMuse(h) {
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

// IsCodexUserAgent reports whether ua has the Codex user-agent shape (any originator).
func IsCodexUserAgent(ua string) bool { return codexUserAgent.MatchString(strings.TrimSpace(ua)) }

// isMuse mirrors internal/tmx/muse.IsClientHeaders (not imported: muse depends on the executors,
// which use this package).
func isMuse(h http.Header) bool {
	ua := strings.ToLower(strings.TrimSpace(h.Get("User-Agent")))
	return strings.HasPrefix(ua, "muse-build/") || strings.HasPrefix(ua, "muse-code/") ||
		strings.HasPrefix(strings.ToLower(strings.TrimSpace(h.Get("X-Client-Id"))), "tbh:")
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

// ResolvesToClaude reports whether a client model id is a Claude model by any route: its id, the
// providers given, the providers the registry has for it, or the upstream model it aliases (an
// openai-compatibility alias "review" for "anthropic/claude-sonnet-5" counts).
func ResolvesToClaude(id string, providers []string) bool {
	if IsClaudeModel(id, providers) {
		return true
	}
	id = stripThinkingSuffix(strings.TrimSpace(id))
	if id == "" {
		return false
	}
	if aliasHasClaudeTarget(id) || aliasHasClaudeTarget(stripThinkingSuffix(strings.TrimSpace(id))) {
		return true
	}
	r := registry.GetGlobalRegistry()
	providers = r.GetModelProviders(id)
	if IsClaudeModel("", providers) {
		return true
	}
	// Every provider's registration: two entries can share an alias with different upstreams, and
	// the registry's default info is only the last one registered.
	for _, p := range append(providers, "") {
		if info := r.GetModelInfo(id, p); info != nil && strings.Contains(strings.ToLower(info.MetadataModelID), "claude") {
			return true
		}
	}
	return false
}

// stripThinkingSuffix drops a trailing "(level)" thinking suffix ("review(high)" -> "review").
func stripThinkingSuffix(id string) string {
	if i := strings.LastIndex(id, "("); i > 0 && strings.HasSuffix(id, ")") {
		return id[:i]
	}
	return id
}

// aliasTargets holds every upstream model each configured alias routes to, per
// openai-compatibility entry: one entry can pool an alias over several upstream names, and the
// registry keeps only the first one's metadata.
var (
	aliasMu      sync.RWMutex
	aliasTargets = map[string]map[string][]string{} // entry -> client alias -> upstream names
)

// SetAliasTargets replaces an openai-compatibility entry's alias -> upstream names (called when its
// models are built from config).
func SetAliasTargets(entry string, targets map[string][]string) {
	entry = strings.ToLower(strings.TrimSpace(entry))
	aliasMu.Lock()
	defer aliasMu.Unlock()
	if len(targets) == 0 {
		delete(aliasTargets, entry)
		return
	}
	aliasTargets[entry] = targets
}

func aliasHasClaudeTarget(id string) bool {
	aliasMu.RLock()
	defer aliasMu.RUnlock()
	for _, byAlias := range aliasTargets {
		for _, name := range byAlias[id] {
			if IsClaudeModel(name, nil) {
				return true
			}
		}
	}
	return false
}

// HiddenPrefixes are client model ids no model list shows: OpenCode Zen (pay as you go) is a
// backend only until the owner picks models to expose. A request by name still routes when the
// entry is enabled.
var HiddenPrefixes = []string{"opencode-zen/"}

// Hidden reports whether a model id is left out of every model list.
func Hidden(id string) bool {
	lower := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(id)), "models/")
	for _, p := range HiddenPrefixes {
		if strings.HasPrefix(lower, p) {
			return true
		}
	}
	return false
}

// modelID reads a list entry's id: "id", the Codex catalog's "slug", or Gemini's "name"
// ("models/<id>").
func modelID(m map[string]any) string {
	for _, k := range []string{"id", "slug", "name"} {
		if v, _ := m[k].(string); v != "" {
			return strings.TrimPrefix(v, "models/")
		}
	}
	return ""
}

// FilterModels drops hidden models from a model list, and Claude models too when the list is for a
// Codex client.
func FilterModels(models []map[string]any, codex bool) []map[string]any {
	out := make([]map[string]any, 0, len(models))
	for _, m := range models {
		id := modelID(m)
		if Hidden(id) {
			continue
		}
		if codex {
			owner, _ := m["owned_by"].(string)
			if ResolvesToClaude(id, []string{owner}) {
				continue
			}
		}
		out = append(out, m)
	}
	return out
}

// FilterModelsForCodex is FilterModels for a Codex client.
func FilterModelsForCodex(models []map[string]any) []map[string]any {
	return FilterModels(models, true)
}
