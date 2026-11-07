package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	proxyconfig "github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/logging"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/capture"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v8/sdk/config"
)

// tokenmaxx fork: the ChatGPT-mode stubs Codex calls (internal/tmx/codexchatgpt). Analytics needs
// the client key; MCP gets none from Codex and needs none. Both answer 2xx and are not archived.
// A bad key on analytics and every refresh call stay archived (they show as rejected rows).
func TestTMXCodexChatGPTRoutes(t *testing.T) {
	var archived []string
	capture.SetSink(func(ex *capture.Exchange) { archived = append(archived, ex.Method+" "+ex.URL); ex.Release() })
	defer capture.SetSink(nil)
	logsDir := t.TempDir()
	server := newTestServerWithConfig(t, &proxyconfig.Config{SDKConfig: sdkconfig.SDKConfig{APIKeys: []string{"test-key"}}},
		WithRequestLoggerFactory(func(*proxyconfig.Config, string) logging.RequestLogger {
			return logging.NewFileRequestLogger(false, logsDir, logsDir, 0)
		}))
	do := func(method, path, key, body string) int {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		if key != "" {
			req.Header.Set("Authorization", "Bearer "+key)
		}
		w := httptest.NewRecorder()
		server.engine.ServeHTTP(w, req)
		return w.Code
	}
	mcpInit := `{"jsonrpc":"2.0","id":0,"method":"initialize","params":{"protocolVersion":"2025-06-18"}}`
	for _, tc := range []struct {
		method, path, key, body string
		want                    int
	}{
		{http.MethodPost, "/backend-api/codex/analytics-events/events", "test-key", `{"events":[]}`, http.StatusOK},
		{http.MethodPost, "/backend-api/ps/mcp", "", mcpInit, http.StatusOK},
		{http.MethodPost, "/backend-api/ps/mcp", "", `{"jsonrpc":"2.0","method":"notifications/initialized"}`, http.StatusAccepted},
		{http.MethodGet, "/backend-api/ps/mcp", "", "", http.StatusMethodNotAllowed},
	} {
		if got := do(tc.method, tc.path, tc.key, tc.body); got != tc.want {
			t.Errorf("%s %s = %d, want %d", tc.method, tc.path, got, tc.want)
		}
	}
	if len(archived) != 0 {
		t.Fatalf("authenticated stub calls were archived: %v", archived)
	}

	if got := do(http.MethodPost, "/backend-api/codex/analytics-events/events", "wrong-key", `{}`); got != http.StatusUnauthorized {
		t.Fatalf("bad key on analytics = %d, want 401", got)
	}
	if got := do(http.MethodPost, "/backend-api/codex/analytics-events/events", "", `{}`); got != http.StatusUnauthorized {
		t.Fatalf("no key on analytics = %d, want 401", got)
	}
	if got := do(http.MethodPost, "/backend-api/tmx/oauth/token", "", `{"grant_type":"refresh_token","refresh_token":"none"}`); got != http.StatusUnauthorized {
		t.Fatalf("refresh = %d, want 401", got)
	}
	want := []string{"POST /backend-api/codex/analytics-events/events", "POST /backend-api/codex/analytics-events/events", "POST /backend-api/tmx/oauth/token"}
	if strings.Join(archived, ",") != strings.Join(want, ",") {
		t.Fatalf("archived %v, want %v (rejections and refreshes stay visible)", archived, want)
	}
}
