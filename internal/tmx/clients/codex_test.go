package clients

import (
	"net/http"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
)

func TestIsCodex(t *testing.T) {
	for _, tc := range []struct {
		ua, originator, clientID string
		want                     bool
	}{
		{"T3 Code/0.166.0 (Ubuntu 24.4.0; x86_64) unknown (T3 Code; 0.1.7-nightly.20261102.4066)", "", "", true},
		{"T3 Code/0.165.0 (Mac OS 26.2.0; arm64) unknown (T3 Code; 0.1.4-nightly.20261028.3824)", "", "", true},
		{"codex_exec/0.165.0 (Ubuntu 24.4.0; x86_64) dumb (codex_exec; 0.165.0)", "", "", true},
		{"codex-tui/0.149.1 (Mac OS 26.5.2; arm64) iTerm.app/3.6.11 (codex-tui; 0.149.1)", "", "", true},
		{"codex_cli_rs/0.130.0", "", "", true},
		{"", "codex_cli_rs", "", true},
		{"claude-cli/2.1.280 (external, cli)", "", "", false},
		{"claude-cli/2.1.280 (external, sdk-ts, agent-sdk/0.2.1)", "", "", false},
		// Claude Code never counts, even with an Originator header.
		{"claude-cli/2.1.280 (external, cli)", "codex_cli_rs", "", false},
		{"muse-build/1.4.11 (non-interactive; linux)", "", "", false},
		{"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0 Safari/537.36", "", "", false},
		{"curl/8.5.0", "", "", false},
		{"opencode/1.2.3", "", "", false},
		{"", "", "tbh:exec", false},
		{"", "", "", false},
	} {
		h := http.Header{}
		if tc.ua != "" {
			h.Set("User-Agent", tc.ua)
		}
		if tc.originator != "" {
			h.Set("Originator", tc.originator)
		}
		if tc.clientID != "" {
			h.Set("X-Client-Id", tc.clientID)
		}
		if got := IsCodex(h); got != tc.want {
			t.Errorf("IsCodex(ua=%q originator=%q client=%q) = %v, want %v", tc.ua, tc.originator, tc.clientID, got, tc.want)
		}
	}
}

func TestIsClaudeModel(t *testing.T) {
	for _, tc := range []struct {
		id        string
		providers []string
		want      bool
	}{
		{"claude-opus-5-5", nil, true},
		{"Claude-Haiku-5-5", nil, true},
		{"anthropic/claude-sonnet-5", []string{"openrouter"}, true},
		{"opencode-zen/claude-fable-5-1", []string{"opencode-zen"}, true},
		{"big-brain", []string{"claude"}, true},
		{"gpt-6.1-sol", []string{"codex"}, false},
		{"opencode-go/kimi-k3", []string{"opencode-go"}, false},
		{"muse-spark-1.3", []string{"meta"}, false},
	} {
		if got := IsClaudeModel(tc.id, tc.providers); got != tc.want {
			t.Errorf("IsClaudeModel(%q, %v) = %v, want %v", tc.id, tc.providers, got, tc.want)
		}
	}
}

func TestFilterModelsForCodex(t *testing.T) {
	in := []map[string]any{
		{"id": "gpt-6.1-sol", "owned_by": "openai"},
		{"id": "claude-opus-5-5", "owned_by": "anthropic"},
		{"slug": "claude-haiku-5-5"},
		{"id": "big-brain", "owned_by": "x"},
		{"id": "opencode-go/glm-5.3", "owned_by": "opencode-go"},
	}
	in = append(in, map[string]any{"id": "opencode-zen/glm-5.3"}, map[string]any{"name": "models/opencode-zen/kimi-k3"})
	r := registry.GetGlobalRegistry()
	r.RegisterClient("tmx-clients-claude", "claude", []*registry.ModelInfo{{ID: "big-brain"}})
	// An openai-compatibility alias whose upstream model is Claude (name -> MetadataModelID).
	r.RegisterClient("tmx-clients-or", "openrouter", []*registry.ModelInfo{{ID: "review", MetadataModelID: "anthropic/claude-sonnet-5"}})
	t.Cleanup(func() { r.UnregisterClient("tmx-clients-claude"); r.UnregisterClient("tmx-clients-or") })
	in = append(in, map[string]any{"id": "review", "owned_by": "openrouter"})
	out := FilterModelsForCodex(in)
	if len(out) != 2 || out[0]["id"] != "gpt-6.1-sol" || out[1]["id"] != "opencode-go/glm-5.3" {
		t.Fatalf("codex list = %v", out)
	}
	// Not for Codex: Claude stays, Zen still goes.
	if out := FilterModels(in, false); len(out) != 6 {
		t.Fatalf("plain list = %v", out)
	}
}
