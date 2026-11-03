package cliproxy

import (
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/clients"
)

// One entry pools the alias "review" over a GPT and a Claude upstream. The registry keeps only the
// first one's metadata (GPT), so the alias targets recorded at build time are what keep it from
// Codex (pr-reviewer r1 S3).
func TestSameEntryAliasPoolWithClaudeIsClaude(t *testing.T) {
	compat := &config.OpenAICompatibility{Name: "pooltest", Models: []config.OpenAICompatibilityModel{
		{Name: "gpt-6-luna", Alias: "review"}, {Name: "anthropic/claude-sonnet-5", Alias: "review"},
		{Name: "kimi-k3", Alias: "plain"},
	}}
	models := buildOpenAICompatibilityConfigModels(compat)
	t.Cleanup(func() { clients.SetAliasTargets("pooltest", nil) })
	r := registry.GetGlobalRegistry()
	r.RegisterClient("tmx-pooltest", "openai-compatible-pooltest", models)
	t.Cleanup(func() { r.UnregisterClient("tmx-pooltest") })
	if !clients.ResolvesToClaude("review", nil) || !clients.ResolvesToClaude("review(high)", nil) {
		t.Fatal("an alias pooled over a Claude upstream is not seen as Claude")
	}
	if clients.ResolvesToClaude("plain", nil) {
		t.Fatal("a non-Claude alias read as Claude")
	}
	out := clients.FilterModelsForCodex([]map[string]any{{"id": "review"}, {"id": "plain"}})
	if len(out) != 1 || out[0]["id"] != "plain" {
		t.Fatalf("codex list = %v", out)
	}
	// The entry rebuilt without the Claude target: the alias is allowed again.
	compat.Models = compat.Models[:1]
	buildOpenAICompatibilityConfigModels(compat)
	if clients.ResolvesToClaude("review", nil) {
		t.Fatal("stale alias target kept after the entry changed")
	}
}
