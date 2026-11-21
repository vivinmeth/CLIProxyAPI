package clients

import (
	"net/http"
	"strings"

	"github.com/MoeForge/moego/json"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/client/grokbuild"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
)

// IsGrokShell uses the same client identity as the Grok catalog dispatcher.
func IsGrokShell(h http.Header) bool { return grokbuild.IsGrokShellUserAgent(h.Get("User-Agent")) }

// OnlyXAI requires positive provider evidence for every possible route. Model names and
// owned_by metadata do not establish which executor will serve a request.
func OnlyXAI(providers []string) bool {
	if len(providers) == 0 {
		return false
	}
	for _, p := range providers {
		if !strings.EqualFold(strings.TrimSpace(p), "xai") {
			return false
		}
	}
	return true
}

// ResolvesOnlyToXAI checks execution providers and all registrations of an alias, including
// compat pools whose registry metadata holds only one target. Unknown routes fail closed.
func ResolvesOnlyToXAI(id string, providers []string) bool {
	id = strings.TrimSpace(id)
	if id == "" {
		return false
	}
	r := registry.GetGlobalRegistry()
	baseID := stripThinkingSuffix(id)
	registered := r.GetModelProviders(id)
	if baseID != id {
		registered = append(registered, r.GetModelProviders(baseID)...)
	}
	if len(providers) > 0 && !OnlyXAI(providers) {
		return false
	}
	if !OnlyXAI(registered) {
		return false
	}
	if f := compatSource.Load(); f != nil {
		for _, e := range (*f)() {
			for _, m := range e.Models {
				alias := strings.TrimSpace(m.Alias)
				if alias == "" {
					alias = strings.TrimSpace(m.Name)
				}
				prefixed := strings.TrimSpace(e.Prefix) + "/" + alias
				if id != alias && baseID != alias && (strings.TrimSpace(e.Prefix) == "" || (id != prefixed && baseID != prefixed)) {
					continue
				}
				if !OnlyXAI(r.GetModelProviders(strings.TrimSpace(m.Name))) {
					return false
				}
			}
		}
	}
	return true
}

const GrokModelErrorCode = "non_xai_models_not_for_grok"

// GrokModelErrorBody explains the client boundary using the usual model error envelope.
func GrokModelErrorBody(model string) string {
	body, _ := json.Marshal(map[string]any{"error": map[string]any{
		"code":    GrokModelErrorCode,
		"message": "Grok-shell clients may request only xAI models on this proxy (" + model + "); the model route must resolve exclusively to xAI",
		"type":    "invalid_request_error",
		"param":   "model",
	}})
	return string(body)
}
