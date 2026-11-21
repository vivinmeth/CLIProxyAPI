package clients

import (
	"net/http"
	"testing"

	"github.com/MoeForge/moego/ojson"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
)

func TestGrokProviderIdentityAndAliases(t *testing.T) {
	r := registry.GetGlobalRegistry()
	registrations := []struct {
		client, provider string
		models           []*registry.ModelInfo
	}{
		{"grok-rules-xai", "xai", []*registry.ModelInfo{{ID: "grok-native"}, {ID: "fast", MetadataModelID: "grok-native"}, {ID: "mixed"}, {ID: "pool"}, {ID: "pool(high)"}, {ID: "opaque-pool"}, {ID: "pure-pool"}, {ID: "literal(high)"}}},
		{"grok-rules-codex", "codex", []*registry.ModelInfo{{ID: "grok-impostor", OwnedBy: "xai"}, {ID: "mixed"}, {ID: "gpt-target"}, {ID: "literal(high)"}}},
		{"grok-rules-meta", "meta", []*registry.ModelInfo{{ID: "grok-meta"}}},
	}
	for _, reg := range registrations {
		r.RegisterClient(reg.client, reg.provider, reg.models)
		client := reg.client
		t.Cleanup(func() { r.UnregisterClient(client) })
	}
	SetCompatSource(func() []CompatEntry {
		return []CompatEntry{{Prefix: "route", Models: []CompatModel{
			{Name: "grok-native", Alias: "pool"}, {Name: "gpt-target", Alias: "pool"},
			{Name: "unregistered-target", Alias: "opaque-pool"}, {Name: "grok-native", Alias: "pure-pool"},
		}}}
	})
	t.Cleanup(func() { SetCompatSource(nil) })
	for _, tc := range []struct {
		id        string
		providers []string
		want      bool
	}{
		{"grok-native", nil, true}, {"fast", nil, true}, {"fast(high)", nil, true},
		{"grok-impostor", nil, false}, {"grok-meta", nil, false}, {"mixed", nil, false},
		{"grok-native", []string{"xai", "claude"}, false}, {"grok-native", []string{"meta"}, false},
		{"missing", nil, false}, {"", []string{"xai"}, false}, {"pool", nil, false},
		{"pool(high)", nil, false}, {"missing", []string{"xai"}, false},
		{"opaque-pool", nil, false}, {"pure-pool", nil, true},
		{"literal(high)", nil, false},
	} {
		if got := ResolvesOnlyToXAI(tc.id, tc.providers); got != tc.want {
			t.Errorf("ResolvesOnlyToXAI(%q,%v)=%v want %v", tc.id, tc.providers, got, tc.want)
		}
	}
	// Live config changes must take effect without stale alias state.
	SetCompatSource(nil)
	if !ResolvesOnlyToXAI("pool", nil) {
		t.Fatal("removed compat pool still vetoes xAI registration")
	}
}

func TestGrokIdentityAndError(t *testing.T) {
	for _, tc := range []struct {
		ua   string
		want bool
	}{
		{"grok-shell/0.2.119 (macos; aarch64)", true},
		{"grok-pager/0.2.119 grok-shell/0.2.119 (macos; aarch64)", true},
		{"claude-cli/2.1.280", false}, {"codex_cli_rs/0.130.0", false}, {"muse-code/1.4.11", false}, {"antigravity/1.0", false}, {"curl/8", false},
	} {
		if got := IsGrokShell(http.Header{"User-Agent": {tc.ua}}); got != tc.want {
			t.Errorf("IsGrokShell(%q)=%v", tc.ua, got)
		}
	}
	body := GrokModelErrorBody("bad\"model\n")
	if ojson.Get(body, "error.code").String() != GrokModelErrorCode || ojson.Get(body, "error.param").String() != "model" {
		t.Fatalf("invalid model error: %s", body)
	}
}
