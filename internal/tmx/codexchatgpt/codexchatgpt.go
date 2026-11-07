// Package codexchatgpt answers the ChatGPT-backend calls Codex makes when it runs in ChatGPT login
// mode against the proxy (chatgpt_base_url pointed here), which is how Codex offers its built-in
// image_gen tool through tmx-proxy (wiki/image-generation.md).
//
// Codex in that mode also calls the ChatGPT backend for telemetry, the connectors MCP server and,
// after a 401, a token refresh. None of them carry model traffic; the proxy answers them itself so
// they don't show up as rejected requests and Codex logs no transport errors:
//
//   - POST /backend-api/codex/analytics-events/events: accepted and dropped.
//   - POST /backend-api/ps/mcp: a minimal MCP server (streamable HTTP) with no tools.
//   - POST /backend-api/tmx/oauth/token: the target of CODEX_REFRESH_TOKEN_URL_OVERRIDE. A
//     synthetic login has nothing to refresh, so it answers with an error Codex treats as final
//     (one call, no retry loop, auth.json kept) and the refresh never reaches auth.openai.com.
//
// Codex's GET calls in this mode (plugin lists, wham/accounts/check, wham/settings/user) are left
// to 404: Codex ignores those failures silently and the proxy neither records nor archives GETs.
package codexchatgpt

import (
	"encoding/json"
	"io"
	"net/http"
)

const (
	AnalyticsPath = "/backend-api/codex/analytics-events/events"
	MCPPath       = "/backend-api/ps/mcp"
	RefreshPath   = "/backend-api/tmx/oauth/token"

	// mcpProtocolVersion is answered when the client asks for a version we don't list.
	mcpProtocolVersion = "2025-06-18"
	maxBodyBytes       = 1 << 20
)

var mcpProtocolVersions = map[string]bool{"2024-11-05": true, "2025-03-26": true, "2025-06-18": true, "2025-11-25": true}

// Analytics accepts a batch of Codex analytics events and drops it.
func Analytics(w http.ResponseWriter, r *http.Request) {
	_, _ = io.Copy(io.Discard, http.MaxBytesReader(w, r.Body, maxBodyBytes))
	writeJSON(w, http.StatusOK, map[string]any{})
}

// Refresh refuses a token refresh with refresh_token_invalidated, which Codex 0.167 treats as
// final: it stops retrying and asks for a new login instead of hammering the endpoint.
func Refresh(w http.ResponseWriter, r *http.Request) {
	_, _ = io.Copy(io.Discard, http.MaxBytesReader(w, r.Body, maxBodyBytes))
	writeJSON(w, http.StatusUnauthorized, map[string]any{
		"error":             "refresh_token_invalidated",
		"error_description": "tmx-proxy: a synthetic Codex login cannot be refreshed; check the proxy key in auth.json (wiki/image-generation.md)",
	})
}

type rpcMessage struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
}

// MCP serves an MCP server with no tools, resources or prompts over streamable HTTP: requests get
// a JSON response, notifications and client responses get 202, and GET (the optional server
// stream) gets 405 as the transport allows.
func MCP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var msg rpcMessage
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes)).Decode(&msg); err != nil {
		writeJSON(w, http.StatusBadRequest, rpcError(nil, -32700, "parse error"))
		return
	}
	if len(msg.ID) == 0 || string(msg.ID) == "null" || msg.Method == "" {
		w.WriteHeader(http.StatusAccepted)
		return
	}
	var result any
	switch msg.Method {
	case "initialize":
		var p struct {
			ProtocolVersion string `json:"protocolVersion"`
		}
		_ = json.Unmarshal(msg.Params, &p)
		version := p.ProtocolVersion
		if !mcpProtocolVersions[version] {
			version = mcpProtocolVersion
		}
		result = map[string]any{
			"protocolVersion": version,
			"capabilities":    map[string]any{"tools": map[string]any{"listChanged": false}},
			"serverInfo":      map[string]any{"name": "tmx-proxy", "title": "tmx-proxy (no connectors)", "version": "1"},
		}
	case "ping":
		result = map[string]any{}
	case "tools/list":
		result = map[string]any{"tools": []any{}}
	case "resources/list":
		result = map[string]any{"resources": []any{}}
	case "resources/templates/list":
		result = map[string]any{"resourceTemplates": []any{}}
	case "prompts/list":
		result = map[string]any{"prompts": []any{}}
	default:
		writeJSON(w, http.StatusOK, rpcError(msg.ID, -32601, "method not found"))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"jsonrpc": "2.0", "id": msg.ID, "result": result})
}

func rpcError(id json.RawMessage, code int, message string) map[string]any {
	var anyID any
	if len(id) > 0 {
		anyID = id
	}
	return map[string]any{"jsonrpc": "2.0", "id": anyID, "error": map[string]any{"code": code, "message": message}}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
