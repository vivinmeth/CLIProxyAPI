package cliproxy

import (
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/clients"
)

// An alias pooled over a GPT and a Claude upstream inside one entry, bare or prefixed, is Claude for
// the Codex rule; the targets come from the live config, so removing, disabling or emptying the
// entry clears them (pr-reviewer r1 S3, r2 S3-S4).
func TestSameEntryAliasPoolWithClaudeIsClaude(t *testing.T) {
	cfg := &config.Config{OpenAICompatibility: []config.OpenAICompatibility{
		{Name: "pool", Prefix: "pool", Models: []config.OpenAICompatibilityModel{
			{Name: "gpt-6-luna", Alias: "review"}, {Name: "anthropic/claude-sonnet-5", Alias: "review"}}},
		{Name: "gptonly", Models: []config.OpenAICompatibilityModel{{Name: "gpt-6-luna", Alias: "shared"}, {Name: "kimi-k3", Alias: "plain"}}},
		{Name: "claudeshared", Models: []config.OpenAICompatibilityModel{{Name: "anthropic/claude-sonnet-5", Alias: "shared"}}},
	}}
	clients.SetCompatSource(func() []clients.CompatEntry { return tmxCompatEntries(cfg) })
	t.Cleanup(func() { clients.SetCompatSource(nil) })
	r := registry.GetGlobalRegistry()
	r.RegisterClient("tmx-pool", "openai-compatible-pool", applyModelPrefixes(buildOpenAICompatibilityConfigModels(&cfg.OpenAICompatibility[0]), "pool", false))
	t.Cleanup(func() { r.UnregisterClient("tmx-pool") })
	for _, id := range []string{"review", "review(high)", "pool/review", "pool/review(high)", "shared"} {
		if !clients.ResolvesToClaude(id, nil) {
			t.Errorf("%s not seen as Claude", id)
		}
	}
	if clients.ResolvesToClaude("plain", nil) {
		t.Error("a non-Claude alias read as Claude")
	}
	out := clients.FilterModelsForCodex([]map[string]any{{"id": "pool/review"}, {"id": "review"}, {"id": "plain"}})
	if len(out) != 1 || out[0]["id"] != "plain" {
		t.Fatalf("codex list = %v", out)
	}
	// The Claude-mapping entry disabled, then removed: the GPT-only "shared" is allowed again.
	cfg.OpenAICompatibility[2].Disabled = true
	if clients.ResolvesToClaude("shared", nil) {
		t.Error("disabled entry still makes shared Claude")
	}
	cfg.OpenAICompatibility = cfg.OpenAICompatibility[:2]
	if clients.ResolvesToClaude("shared", nil) {
		t.Error("removed entry still makes shared Claude")
	}
	// The pooled entry emptied of models: its alias targets are gone.
	cfg.OpenAICompatibility[0].Models = nil
	r.UnregisterClient("tmx-pool")
	if clients.ResolvesToClaude("pool/review", nil) || clients.ResolvesToClaude("review", nil) {
		t.Error("emptied entry still makes review Claude")
	}
}
