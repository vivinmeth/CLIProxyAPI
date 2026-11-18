package api

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/MoeForge/moego/json"
	"github.com/gin-gonic/gin"
	proxyconfig "github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/logging"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/capture"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/codexchatgpt"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v8/sdk/config"
)

// tokenmaxx fork: the ChatGPT-mode stubs Codex calls (internal/tmx/codexchatgpt). Analytics needs
// the client key; MCP gets none from Codex and needs none. Both answer 2xx and are not archived.
// A bad key on analytics stays archived; refreshes and account checks never do.
func TestTMXCodexChatGPTRoutes(t *testing.T) {
	var archived []string
	capture.SetSink(func(ex *capture.Exchange) { archived = append(archived, ex.Method+" "+ex.URL); ex.Release() })
	defer capture.SetSink(nil)
	logsDir := t.TempDir()
	server := newTestServerWithConfig(t, &proxyconfig.Config{SDKConfig: sdkconfig.SDKConfig{APIKeys: []string{"test-key"}}},
		WithMiddleware(func(c *gin.Context) {
			c.Next()
			if c.Request.URL.Path == codexchatgpt.RefreshPath || c.Request.URL.Path == codexchatgpt.AccountsPath {
				if !c.GetBool("tmx.capture.skip") {
					t.Error("sensitive stub route did not set TMXSkipCapture")
				}
			}
		}),
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
		{http.MethodPost, codexchatgpt.RefreshPath, "", `{"grant_type":"refresh_token","refresh_token":"` + codexchatgpt.PlaceholderRefreshToken + `"}`, http.StatusOK},
		{http.MethodPost, codexchatgpt.RefreshPath, "", `{"grant_type":"refresh_token","refresh_token":"none"}`, http.StatusUnauthorized},
		{http.MethodGet, codexchatgpt.AccountsPath, "test-key", "", http.StatusOK},
		{http.MethodGet, codexchatgpt.AccountsPath, "wrong-key", "", http.StatusUnauthorized},
		{http.MethodGet, codexchatgpt.AccountsPath, "", "", http.StatusUnauthorized},
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
	want := []string{"POST /backend-api/codex/analytics-events/events", "POST /backend-api/codex/analytics-events/events"}
	if strings.Join(archived, ",") != strings.Join(want, ",") {
		t.Fatalf("archived %v, want %v (only analytics rejections stay visible)", archived, want)
	}
}

func TestTMXCodexAccountsAuthenticatedAndConstrained(t *testing.T) {
	server := newTestServer(t)
	for _, tc := range []struct {
		name, key, accountID, wantID string
		status                       int
	}{
		{"no key", "", "private-account", "", http.StatusUnauthorized},
		{"bad key", "bad-key", "private-account", "", http.StatusUnauthorized},
		{"valid", "test-key", "Account_1.2:team-3", "Account_1.2:team-3", http.StatusOK},
		{"missing", "test-key", "", "tmx-proxy", http.StatusOK},
		{"invalid", "test-key", "https://other.invalid", "tmx-proxy", http.StatusOK},
		{"space", "test-key", "account id", "tmx-proxy", http.StatusOK},
		{"unicode", "test-key", "accóunt", "tmx-proxy", http.StatusOK},
		{"too long", "test-key", strings.Repeat("a", 129), "tmx-proxy", http.StatusOK},
		{"max length", "test-key", strings.Repeat("a", 128), strings.Repeat("a", 128), http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, codexchatgpt.AccountsPath+"?extra=query-must-not-echo", nil)
			if tc.key != "" {
				req.Header.Set("Authorization", "Bearer "+tc.key)
			}
			req.Header.Set("ChatGPT-Account-ID", tc.accountID)
			req.Header.Set("X-Unrelated", "header-must-not-echo")
			req.Header.Set("X-Workspace-Backend-Origin", "https://untrusted.invalid")
			w := httptest.NewRecorder()
			server.engine.ServeHTTP(w, req)
			if w.Code != tc.status {
				t.Fatalf("status = %d, want %d", w.Code, tc.status)
			}
			for _, forbidden := range []string{"private-account", "header-must-not-echo", "query-must-not-echo", "https://untrusted.invalid", "test-key", "bad-key"} {
				if strings.Contains(w.Body.String(), forbidden) {
					t.Fatal("response echoed unrelated or unauthenticated input")
				}
				for _, values := range w.Header() {
					if strings.Contains(strings.Join(values, ","), forbidden) {
						t.Fatal("response headers echoed unrelated or unauthenticated input")
					}
				}
			}
			if tc.status == http.StatusOK {
				var out map[string]any
				if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
					t.Fatal(err)
				}
				accounts, ok := out["accounts"].([]any)
				if len(out) != 1 || !ok || len(accounts) != 1 {
					t.Fatal("expected only one synthetic account")
				}
				account, ok := accounts[0].(map[string]any)
				if !ok || len(account) != 3 || account["id"] != tc.wantID || account["workspace_backend_origin"] != "NO_CONSTRAINT" || account["account_routing_override"] != "NO_CONSTRAINT" {
					t.Fatal("unexpected synthetic account or routing constraint")
				}
			}
		})
	}
}

func TestTMXCodexRefreshNeverWritesRequestLogs(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "error-only", true: "full"}[enabled], func(t *testing.T) {
			logsDir := t.TempDir()
			server := newTestServerWithOptions(t, WithRequestLoggerFactory(func(*proxyconfig.Config, string) logging.RequestLogger {
				return logging.NewFileRequestLogger(enabled, logsDir, logsDir, 0)
			}))
			for _, token := range []string{codexchatgpt.PlaceholderRefreshToken, "rt_test_revoked_0123456789"} {
				req := httptest.NewRequest(http.MethodPost, codexchatgpt.RefreshPath, strings.NewReader(`{"grant_type":"refresh_token","refresh_token":"`+token+`"}`))
				server.engine.ServeHTTP(httptest.NewRecorder(), req)
			}
			entries, err := os.ReadDir(logsDir)
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != 0 {
				t.Fatal("refresh route wrote a request log")
			}
			// Prove the configured logger really writes other routes in both modes.
			req := httptest.NewRequest(http.MethodPost, codexchatgpt.AnalyticsPath, strings.NewReader(`{}`))
			req.Header.Set("Authorization", "Bearer wrong-key")
			server.engine.ServeHTTP(httptest.NewRecorder(), req)
			entries, err = os.ReadDir(logsDir)
			if err != nil || len(entries) == 0 {
				t.Fatal("control rejection did not write a request log")
			}
		})
	}
}
