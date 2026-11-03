package requestlog

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	coreusage "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/usage"
)

// No API key value, upstream or client, may reach a stored row (the request log is mirrored to
// analytics, which the agents role reads).
func TestNoKeyValueReachesARow(t *testing.T) {
	const upstream = "oc_sk_0123456789ab_FAKEFAKEFAKEFAKEFAKEFAKEFAKEFAKE"
	const client = "sk-tmx-alien-FAKEFAKEFAKEFAKEFAKE"
	for name, rec := range map[string]coreusage.Record{
		"api-key credential": {Provider: "openai-compatible-opencode-go", Model: "kimi-k3", AuthType: "apikey", Source: upstream, APIKey: client},
		"no credential":      {Provider: "openai-compatible-opencode-go", Model: "kimi-k3", Source: client, APIKey: client},
		"unknown auth type":  {Provider: "x", Model: "m", Source: "sk-ant-api03-FAKEFAKEFAKEFAKE", APIKey: client},
	} {
		row := rowFromRecord(context.Background(), rec)
		raw, _ := json.Marshal(row)
		for _, secret := range []string{upstream, client, "sk-ant-api03-FAKEFAKEFAKEFAKE"} {
			if strings.Contains(string(raw), secret) {
				t.Errorf("%s: row carries a key: %s", name, raw)
			}
		}
	}
	// The same key always gets the same label; an email or project id stays readable.
	a := rowFromRecord(context.Background(), coreusage.Record{AuthType: "apikey", Source: upstream})
	b := rowFromRecord(context.Background(), coreusage.Record{AuthType: "apikey", Source: upstream})
	if a.AuthLabel != b.AuthLabel || !strings.HasPrefix(a.AuthLabel, "key:") || len(a.AuthLabel) != 16 {
		t.Fatalf("labels %q %q", a.AuthLabel, b.AuthLabel)
	}
	for _, keep := range []string{"someone@example.com", "my-vertex-project"} {
		if r := rowFromRecord(context.Background(), coreusage.Record{AuthType: "oauth", Source: keep}); r.AuthLabel != keep {
			t.Errorf("label %q became %q", keep, r.AuthLabel)
		}
	}
}
