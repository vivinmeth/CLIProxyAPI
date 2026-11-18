package codexchatgpt

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/MoeForge/moego/json"
	"github.com/klauspost/compress/zstd"
	log "github.com/sirupsen/logrus"
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

func TestRefreshBoundsLogFields(t *testing.T) {
	var logs bytes.Buffer
	logger := log.StandardLogger()
	oldOutput, oldFormatter, oldLevel := logger.Out, logger.Formatter, logger.GetLevel()
	logger.SetOutput(&logs)
	logger.SetFormatter(&log.JSONFormatter{})
	logger.SetLevel(log.InfoLevel)
	t.Cleanup(func() {
		logger.SetOutput(oldOutput)
		logger.SetFormatter(oldFormatter)
		logger.SetLevel(oldLevel)
	})

	huge := strings.Repeat("x", 1<<18)
	for _, tc := range []struct {
		name, clientID, grantType, reason   string
		wantClientID, wantGrant, wantReason string
		wantStatus                          int
	}{
		{"huge client", huge, "refresh_token", "scheduled", huge[:64], "refresh_token", "scheduled", http.StatusOK},
		{"huge reason", "app_x", "refresh_token", huge, "app_x", "refresh_token", "other", http.StatusOK},
		{"all huge", huge, huge, huge, huge[:64], "other", "other", http.StatusUnauthorized},
		{"missing reason", "app_x", "refresh_token", "", "app_x", "refresh_token", "missing", http.StatusOK},
		{"proactive expiry", "app_x", "refresh_token", "proactive_expiry", "app_x", "refresh_token", "proactive_expiry", http.StatusOK},
		{"forced", "app_x", "refresh_token", "forced", "app_x", "refresh_token", "forced", http.StatusOK},
		{"unauthorized recovery", "app_x", "refresh_token", "unauthorized_recovery", "app_x", "refresh_token", "unauthorized_recovery", http.StatusUnauthorized},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logs.Reset()
			body, err := json.Marshal(map[string]string{
				"client_id": tc.clientID, "grant_type": tc.grantType,
				"refresh_reason": tc.reason, "refresh_token": PlaceholderRefreshToken,
			})
			if err != nil {
				t.Fatal(err)
			}
			w, _ := call(t, Refresh, http.MethodPost, string(body))
			if w.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d", w.Code, tc.wantStatus)
			}
			if logs.Len() > 1024 || bytes.Count(logs.Bytes(), []byte("\n")) != 1 {
				t.Fatalf("expected one bounded log entry, got %d bytes", logs.Len())
			}
			var fields map[string]any
			if err := json.Unmarshal(logs.Bytes(), &fields); err != nil {
				t.Fatal(err)
			}
			for key, want := range map[string]string{
				"client_id": tc.wantClientID, "grant_type": tc.wantGrant, "refresh_reason": tc.wantReason,
			} {
				if fields[key] != want {
					t.Errorf("logged %s differs from expected bounded value", key)
				}
			}
			if fields["placeholder_match"] != true || fields["status"] != float64(tc.wantStatus) {
				t.Fatal("placeholder match or status missing or incorrect")
			}
			if strings.Contains(logs.String(), PlaceholderRefreshToken) || fields["refresh_token"] != nil || fields["body"] != nil {
				t.Fatal("refresh log contains token or body data")
			}
		})
	}
}

func TestRefreshOnlyKeepsExactSyntheticLogin(t *testing.T) {
	zstdEncoder, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer zstdEncoder.Close()
	valid := `{"client_id":"app_x","grant_type":"refresh_token","refresh_token":"` + PlaceholderRefreshToken + `","refresh_reason":"scheduled"}`
	for _, tc := range []struct {
		name, body, encoding string
		want                 int
	}{
		{"scheduled", valid, "", http.StatusOK},
		{"missing reason", strings.ReplaceAll(valid, `,"refresh_reason":"scheduled"`, ""), "", http.StatusOK},
		{"unknown reason", strings.ReplaceAll(valid, "scheduled", "new_reason"), "", http.StatusOK},
		{"recovery", strings.ReplaceAll(valid, "scheduled", "unauthorized_recovery"), "", http.StatusUnauthorized},
		{"suffix", strings.ReplaceAll(valid, PlaceholderRefreshToken, PlaceholderRefreshToken+"-extra"), "", http.StatusUnauthorized},
		{"prefix", strings.ReplaceAll(valid, PlaceholderRefreshToken, "tmx-synthetic"), "", http.StatusUnauthorized},
		{"case", strings.ReplaceAll(valid, PlaceholderRefreshToken, strings.ToUpper(PlaceholderRefreshToken)), "", http.StatusUnauthorized},
		{"real-looking", strings.ReplaceAll(valid, PlaceholderRefreshToken, "rt_test_revoked_0123456789"), "", http.StatusUnauthorized},
		{"empty token", strings.ReplaceAll(valid, PlaceholderRefreshToken, ""), "", http.StatusUnauthorized},
		{"wrong grant", strings.ReplaceAll(valid, `"grant_type":"refresh_token"`, `"grant_type":"authorization_code"`), "", http.StatusUnauthorized},
		{"empty body", "", "", http.StatusUnauthorized},
		{"not JSON", "not json", "", http.StatusUnauthorized},
		{"trailing JSON", valid + `{}`, "", http.StatusUnauthorized},
		{"invalid field type", strings.ReplaceAll(valid, `"scheduled"`, `42`), "", http.StatusUnauthorized},
		{"zstd", string(zstdEncoder.EncodeAll([]byte(valid), nil)), "zstd", http.StatusUnauthorized},
		{"zstd header", valid, "zstd", http.StatusUnauthorized},
		{"too large", valid + strings.Repeat(" ", maxBodyBytes), "", http.StatusUnauthorized},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var logs bytes.Buffer
			logger := log.StandardLogger()
			oldOutput, oldFormatter, oldLevel := logger.Out, logger.Formatter, logger.GetLevel()
			logger.SetOutput(&logs)
			logger.SetFormatter(&log.JSONFormatter{})
			logger.SetLevel(log.InfoLevel)
			defer func() {
				logger.SetOutput(oldOutput)
				logger.SetFormatter(oldFormatter)
				logger.SetLevel(oldLevel)
			}()
			req := httptest.NewRequest(http.MethodPost, RefreshPath, strings.NewReader(tc.body))
			req.Header.Set("Content-Encoding", tc.encoding)
			w := httptest.NewRecorder()
			Refresh(w, req)
			if w.Code != tc.want {
				t.Fatalf("status = %d, want %d", w.Code, tc.want)
			}
			if tc.want == http.StatusOK {
				if w.Body.String() != "{}" {
					t.Fatal("success must be exactly an empty JSON object")
				}
			} else {
				var out map[string]any
				if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil || out["error"] != "refresh_token_invalidated" {
					t.Fatal("expected the existing final refresh error")
				}
			}
			if bytes.Count(logs.Bytes(), []byte("\n")) != 1 {
				t.Fatal("expected one structured log line")
			}
			for _, forbidden := range []string{PlaceholderRefreshToken, "rt_test_revoked_0123456789", `"refresh_token":`, `"body":`} {
				if strings.Contains(logs.String(), forbidden) {
					t.Fatal("refresh log contains token or body data")
				}
			}
			var fields map[string]any
			if err := json.Unmarshal(logs.Bytes(), &fields); err != nil {
				t.Fatal(err)
			}
			for _, key := range []string{"grant_type", "client_id", "refresh_reason", "placeholder_match", "status"} {
				if _, ok := fields[key]; !ok {
					t.Errorf("missing log field %s", key)
				}
			}
			if fields["status"] != float64(tc.want) {
				t.Fatal("logged status does not match response")
			}
		})
	}
}
