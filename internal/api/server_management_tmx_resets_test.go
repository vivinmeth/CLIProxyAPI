package api

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/resets"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

// resetsTestBackend serves canned provider answers for one Codex account.
type resetsTestBackend struct{}

func (resetsTestBackend) TmxAuths() []*coreauth.Auth {
	return []*coreauth.Auth{{ID: "codex-1", Provider: "codex", Label: "c@example", Metadata: map[string]any{"account_id": "acct"}}}
}

func (resetsTestBackend) TmxSend(_ context.Context, _ *coreauth.Auth, method, url string, _ map[string]string, _ []byte) (int, []byte, error) {
	switch {
	case strings.HasSuffix(url, "/wham/usage"):
		return 200, []byte(`{"rate_limit":{"primary_window":{"used_percent":0,"limit_window_seconds":604800,"reset_at":4102444800}},"rate_limit_reset_credits":{"applicable_available_count":0}}`), nil
	case strings.HasSuffix(url, "/rate-limit-reset-credits"):
		return 200, []byte(`{"credits":[{"id":"c1","reset_type":"codex_rate_limits","status":"available","expires_at":"2099-01-01T00:00:00Z","title":"Full reset"}]}`), nil
	}
	return 500, nil, nil
}

func (resetsTestBackend) TmxClearRecoveredQuota(context.Context, string, coreauth.QuotaRecovery) error {
	return nil
}

func TestTMXResetRoutesContract(t *testing.T) {
	gin.SetMode(gin.TestMode)
	resets.SetDefaultForTest(nil)
	t.Cleanup(func() { resets.SetDefaultForTest(nil) })
	r := gin.New()
	registerTMXResetRoutes(r.Group("/v8/management"), resetsTestBackend{})
	call := func(method, path, body string) (int, map[string]any) {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		var out map[string]any
		_ = json.Unmarshal(w.Body.Bytes(), &out)
		return w.Code, out
	}

	code, list := call("GET", "/v8/management/tmx/resets?provider=codex", "")
	if code != 200 || list["enabled"] != true {
		t.Fatalf("list: %d %v", code, list)
	}
	for _, k := range []string{"generated_at", "summary", "accounts", "history"} {
		if _, ok := list[k]; !ok {
			t.Errorf("list missing %q", k)
		}
	}
	accounts := list["accounts"].([]any)
	if len(accounts) != 1 {
		t.Fatalf("accounts: %v", accounts)
	}
	credit := accounts[0].(map[string]any)["credits"].([]any)[0].(map[string]any)
	if v := credit["verdict"].(map[string]any); v["kind"] != "nothing" || v["spendable"] != true {
		t.Fatalf("verdict: %v", v)
	}
	if _, empty := call("GET", "/v8/management/tmx/resets?provider=claude", ""); len(empty["accounts"].([]any)) != 0 {
		t.Fatal("provider filter")
	}

	code, poll := call("POST", "/v8/management/tmx/resets/poll?auth_id=codex-1", "")
	if code != 200 || poll["throttled"] != true || poll["account"] == nil {
		t.Fatalf("poll right after a read must be throttled: %d %v", code, poll)
	}
	if code, _ = call("POST", "/v8/management/tmx/resets/poll?auth_id=nope", ""); code != 404 {
		t.Fatalf("poll unknown: %d", code)
	}

	code, hist := call("GET", "/v8/management/tmx/resets/history?limit=5", "")
	if code != 200 || hist["next_before_id"] != float64(0) {
		t.Fatalf("history: %d %v", code, hist)
	}
	if code, _ = call("GET", "/v8/management/tmx/resets/history?from=yesterday", ""); code != 400 {
		t.Fatalf("bad from: %d", code)
	}

	if code, _ = call("POST", "/v8/management/tmx/resets/redeem", `{"auth_id":"codex-1"}`); code != 400 {
		t.Fatalf("redeem without credit_id: %d", code)
	}
	code, refused := call("POST", "/v8/management/tmx/resets/redeem", `{"auth_id":"codex-1","credit_id":"gone"}`)
	if code != 409 || refused["blocker"] != "unknown_credit" {
		t.Fatalf("redeem unknown credit: %d %v", code, refused)
	}
	if code, _ = call("POST", "/v8/management/tmx/resets/redeem", `{"auth_id":"nope","credit_id":"c1"}`); code != 404 {
		t.Fatalf("redeem unknown account: %d", code)
	}
}

func TestRedeemActor(t *testing.T) {
	if got := redeemActor("", "10.0.0.1"); got != "10.0.0.1" {
		t.Fatal(got)
	}
	if got := redeemActor(" fleet-ui:vivin\n", "10.0.0.1"); got != "fleet-ui:vivin @ 10.0.0.1" {
		t.Fatal(got)
	}
	if got := redeemActor(strings.Repeat("x", 100), "ip"); len(got) != 64+len(" @ ip") {
		t.Fatalf("not bounded: %d", len(got))
	}
}

// Without the request-log database there is no balance history: enabled false, never an error.
func TestTMXResetsBalanceHistoryWithoutDatabase(t *testing.T) {
	gin.SetMode(gin.TestMode)
	resets.SetDefaultForTest(nil)
	t.Cleanup(func() { resets.SetDefaultForTest(nil) })
	r := gin.New()
	registerTMXResetRoutes(r.Group("/v8/management"), resetsTestBackend{})
	for path, want := range map[string]int{
		"/v8/management/tmx/resets/balance-history":            200,
		"/v8/management/tmx/resets/balance-history?from=x":     400,
		"/v8/management/tmx/resets/balance-history?after_id=y": 400,
	} {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		if w.Code != want {
			t.Fatalf("%s: %d %s", path, w.Code, w.Body.String())
		}
		if want == 200 && w.Body.String() != `{"enabled":false,"items":[],"next_after_id":0}` {
			t.Fatalf("%s: %s", path, w.Body.String())
		}
	}
}
