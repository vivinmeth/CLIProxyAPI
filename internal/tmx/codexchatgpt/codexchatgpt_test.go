package codexchatgpt

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func call(t *testing.T, h http.HandlerFunc, method, body string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	w := httptest.NewRecorder()
	h(w, httptest.NewRequest(method, "/x", strings.NewReader(body)))
	var out map[string]any
	if w.Body.Len() > 0 {
		if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
			t.Fatalf("body %q is not JSON: %v", w.Body.String(), err)
		}
	}
	return w, out
}

// Codex 0.167 sends exactly this initialize; the reply must be a valid MCP result or Codex logs a
// fatal transport error for the connectors server.
func TestMCPInitializeEchoesKnownVersion(t *testing.T) {
	w, out := call(t, MCP, http.MethodPost, `{"jsonrpc":"2.0","id":0,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{"elicitation":{"form":{},"url":{}}},"clientInfo":{"name":"codex-mcp-client","title":"Codex","version":"0.167.0"}}}`)
	if w.Code != http.StatusOK || w.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("status %d, type %q", w.Code, w.Header().Get("Content-Type"))
	}
	if out["id"] != float64(0) || out["jsonrpc"] != "2.0" {
		t.Fatalf("id/jsonrpc wrong: %v", out)
	}
	result, _ := out["result"].(map[string]any)
	if result["protocolVersion"] != "2025-06-18" {
		t.Fatalf("protocolVersion = %v", result["protocolVersion"])
	}
	if _, ok := result["capabilities"].(map[string]any)["tools"]; !ok {
		t.Fatalf("no tools capability: %v", result)
	}
}

func TestMCPInitializeUnknownVersionFallsBack(t *testing.T) {
	_, out := call(t, MCP, http.MethodPost, `{"jsonrpc":"2.0","id":"a","method":"initialize","params":{"protocolVersion":"2099-01-01"}}`)
	if got := out["result"].(map[string]any)["protocolVersion"]; got != mcpProtocolVersion {
		t.Fatalf("protocolVersion = %v, want %s", got, mcpProtocolVersion)
	}
	if out["id"] != "a" {
		t.Fatalf("string id not echoed: %v", out["id"])
	}
}

func TestMCPListsAreEmpty(t *testing.T) {
	for method, key := range map[string]string{"tools/list": "tools", "resources/list": "resources", "resources/templates/list": "resourceTemplates", "prompts/list": "prompts"} {
		_, out := call(t, MCP, http.MethodPost, `{"jsonrpc":"2.0","id":7,"method":"`+method+`"}`)
		list, ok := out["result"].(map[string]any)[key].([]any)
		if !ok || len(list) != 0 {
			t.Fatalf("%s: %s = %v, want []", method, key, out["result"])
		}
	}
}

func TestMCPNotificationsAndOthers(t *testing.T) {
	if w, _ := call(t, MCP, http.MethodPost, `{"jsonrpc":"2.0","method":"notifications/initialized"}`); w.Code != http.StatusAccepted || w.Body.Len() != 0 {
		t.Fatalf("notification: %d %q", w.Code, w.Body.String())
	}
	if w, _ := call(t, MCP, http.MethodPost, `{"jsonrpc":"2.0","id":3,"result":{}}`); w.Code != http.StatusAccepted {
		t.Fatalf("client response: %d", w.Code)
	}
	if _, out := call(t, MCP, http.MethodPost, `{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"x"}}`); out["error"].(map[string]any)["code"] != float64(-32601) {
		t.Fatalf("unknown method: %v", out)
	}
	if w, out := call(t, MCP, http.MethodPost, `not json`); w.Code != http.StatusBadRequest || out["error"].(map[string]any)["code"] != float64(-32700) {
		t.Fatalf("parse error: %d %v", w.Code, out)
	}
	if w, _ := call(t, MCP, http.MethodGet, ""); w.Code != http.StatusMethodNotAllowed || w.Header().Get("Allow") != http.MethodPost {
		t.Fatalf("GET: %d allow %q", w.Code, w.Header().Get("Allow"))
	}
}

func TestAnalyticsAccepts(t *testing.T) {
	if w, out := call(t, Analytics, http.MethodPost, `{"events":[{"event_type":"codex_turn_event"}]}`); w.Code != http.StatusOK || out == nil {
		t.Fatalf("analytics: %d %q", w.Code, w.Body.String())
	}
}

// refresh_token_invalidated is one of the codes Codex treats as final (no retry loop).
func TestRefreshIsFinal(t *testing.T) {
	w, out := call(t, Refresh, http.MethodPost, `{"client_id":"app_x","grant_type":"refresh_token","refresh_reason":"unauthorized_recovery","refresh_token":"none"}`)
	if w.Code != http.StatusUnauthorized || out["error"] != "refresh_token_invalidated" {
		t.Fatalf("refresh: %d %v", w.Code, out)
	}
}
