package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/clients"
)

// The owner's rule (tokenmaxx fork): no Claude model reaches a Codex client, by model list, alias
// or a direct request by name. Claude Code still sees and reaches Claude models.

const (
	tmxUAT3Codex  = "T3 Code/0.166.0 (Ubuntu 24.4.0; x86_64) unknown (T3 Code; 0.1.7-nightly.20261102.4066)"
	tmxUACodexCLI = "codex_exec/0.165.0 (Ubuntu 24.4.0; x86_64) dumb (codex_exec; 0.165.0)"
	tmxUAClaude   = "claude-cli/2.1.280 (external, cli)"
)

func registerCodexClaudeFixture(t *testing.T) {
	t.Helper()
	r := registry.GetGlobalRegistry()
	r.RegisterClient("tmx-cc-claude", "claude", []*registry.ModelInfo{
		{ID: "claude-opus-5-5", OwnedBy: "anthropic"},
		// An alias that hides "claude" in its name but is served by the Claude provider.
		{ID: "big-brain", OwnedBy: "anthropic"},
	})
	r.RegisterClient("tmx-cc-codex", "codex", []*registry.ModelInfo{{ID: "gpt-6.1-sol", OwnedBy: "openai"}})
	r.RegisterClient("tmx-cc-go", "opencode-go", []*registry.ModelInfo{{ID: "opencode-go/kimi-k3", OwnedBy: "opencode-go"}})
	t.Cleanup(func() {
		for _, id := range []string{"tmx-cc-claude", "tmx-cc-codex", "tmx-cc-go"} {
			r.UnregisterClient(id)
		}
	})
}

func tmxModelIDs(t *testing.T, body []byte) []string {
	t.Helper()
	var resp struct {
		Data   []map[string]any `json:"data"`
		Models []map[string]any `json:"models"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("decode model list: %v (%s)", err, body)
	}
	var ids []string
	for _, m := range append(resp.Data, resp.Models...) {
		id, _ := m["id"].(string)
		if id == "" {
			id, _ = m["slug"].(string)
		}
		ids = append(ids, id)
	}
	return ids
}

func TestTmxCodexModelListsHaveNoClaude(t *testing.T) {
	registerCodexClaudeFixture(t)
	server := newTestServer(t)
	for _, tc := range []struct {
		name, path, ua string
	}{
		{"codex catalog, T3 Codex", "/v1/models?client_version=0.166.0", tmxUAT3Codex},
		{"codex catalog, no UA", "/v1/models?client_version=0.166.0", ""},
		{"plain list, codex_exec", "/v1/models", tmxUACodexCLI},
		{"plain list, T3 Codex", "/v1/models", tmxUAT3Codex},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tc.path, nil)
			req.Header.Set("Authorization", "Bearer test-key")
			if tc.ua != "" {
				req.Header.Set("User-Agent", tc.ua)
			}
			rec := httptest.NewRecorder()
			server.engine.ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
			}
			ids := tmxModelIDs(t, rec.Body.Bytes())
			got := map[string]bool{}
			for _, id := range ids {
				got[id] = true
				if strings.Contains(strings.ToLower(id), "claude") {
					t.Errorf("Codex list has Claude model %q", id)
				}
			}
			if got["big-brain"] {
				t.Errorf("Codex list has the Claude-served alias big-brain")
			}
			for _, want := range []string{"gpt-6.1-sol", "opencode-go/kimi-k3"} {
				if !got[want] {
					t.Errorf("Codex list lost %q: %v", want, ids)
				}
			}
		})
	}
}

func TestTmxClaudeCodeListKeepsClaude(t *testing.T) {
	registerCodexClaudeFixture(t)
	server := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	req.Header.Set("Authorization", "Bearer test-key")
	req.Header.Set("User-Agent", tmxUAClaude)
	rec := httptest.NewRecorder()
	server.engine.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	got := map[string]bool{}
	for _, id := range tmxModelIDs(t, rec.Body.Bytes()) {
		got[id] = true
	}
	for _, want := range []string{"claude-opus-5-5", "big-brain"} {
		if !got[want] && !got["claude-fable-5-dd-"+reverse(want)] {
			t.Errorf("Claude Code list lost %q: %v", want, got)
		}
	}
}

func reverse(s string) string {
	r := []rune(s)
	for i, j := 0, len(r)-1; i < j; i, j = i+1, j-1 {
		r[i], r[j] = r[j], r[i]
	}
	return string(r)
}

func TestTmxCodexClaudeRequestsRefused(t *testing.T) {
	registerCodexClaudeFixture(t)
	server := newTestServer(t)
	post := func(path, ua, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer test-key")
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("User-Agent", ua)
		rec := httptest.NewRecorder()
		server.engine.ServeHTTP(rec, req)
		return rec
	}
	for _, tc := range []struct{ name, path, ua, body string }{
		{"responses by name", "/v1/responses", tmxUAT3Codex, `{"model":"claude-opus-5-5","input":"hi"}`},
		{"responses streaming", "/v1/responses", tmxUACodexCLI, `{"model":"claude-opus-5-5","input":"hi","stream":true}`},
		{"responses via alias", "/v1/responses", tmxUAT3Codex, `{"model":"big-brain","input":"hi"}`},
		{"responses thinking suffix", "/v1/responses", tmxUACodexCLI, `{"model":"claude-opus-5-5(high)","input":"hi"}`},
		{"chat completions", "/v1/chat/completions", tmxUACodexCLI, `{"model":"claude-opus-5-5","messages":[{"role":"user","content":"hi"}]}`},
		{"originator only", "/v1/responses", "", `{"model":"claude-opus-5-5","input":"hi"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var rec *httptest.ResponseRecorder
			if tc.name == "originator only" {
				req := httptest.NewRequest(http.MethodPost, tc.path, strings.NewReader(tc.body))
				req.Header.Set("Authorization", "Bearer test-key")
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("Originator", "codex_cli_rs")
				rec = httptest.NewRecorder()
				server.engine.ServeHTTP(rec, req)
			} else {
				rec = post(tc.path, tc.ua, tc.body)
			}
			if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), clients.CodexClaudeErrorCode) {
				t.Fatalf("want 400 %s, got %d: %s", clients.CodexClaudeErrorCode, rec.Code, rec.Body.String())
			}
		})
	}
	// Claude Code asking for the same model is not refused by this rule (it fails later only for
	// lack of a credential in this test server).
	rec := post("/v1/messages", tmxUAClaude, `{"model":"claude-opus-5-5","max_tokens":8,"messages":[{"role":"user","content":"hi"}]}`)
	if strings.Contains(rec.Body.String(), clients.CodexClaudeErrorCode) {
		t.Fatalf("Claude Code request refused by the Codex rule: %d %s", rec.Code, rec.Body.String())
	}
	// A Codex client still reaches non-Claude models (fails later only for lack of a credential).
	rec = post("/v1/responses", tmxUAT3Codex, `{"model":"gpt-6.1-sol","input":"hi"}`)
	if strings.Contains(rec.Body.String(), clients.CodexClaudeErrorCode) {
		t.Fatalf("Codex request for a GPT model refused: %d %s", rec.Code, rec.Body.String())
	}
}
