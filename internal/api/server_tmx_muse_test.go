package api

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/MoeForge/moego/ojson"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/runtime/executor"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

// tmxSkipUpstreamMuseCatalog skips upstream tests that pin musecode.HandleModels' registry-wide
// catalog: GET /muse-code/models serves Meta's own catalog here (server_tmx_muse.go).
func tmxSkipUpstreamMuseCatalog(t *testing.T) {
	t.Helper()
	t.Skip("tokenmaxx: /muse-code/models serves Meta's catalog (internal/tmx/muse), not upstream's registry catalog")
}

// GET /muse-code/models forwards Meta's catalog through the Meta credential, behind client-key
// auth, while upstream's relay keeps the other /muse-code routes.
func TestTMXMuseCodeModelsServesMetaCatalog(t *testing.T) {
	var catalogCalls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/muse-code/models" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		catalogCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"object":"list","data":[{"id":"muse-wonder-1.0","object":"model"}]}`))
	}))
	defer upstream.Close()

	server := newTestServer(t)
	server.handlers.AuthManager.RegisterExecutor(executor.NewMetaExecutor(server.getConfig()))
	cred := &auth.Auth{
		ID:       "meta-tmx-muse-catalog",
		Provider: "meta",
		Status:   auth.StatusActive,
		Attributes: map[string]string{
			"api_key":  "meta-upstream-secret",
			"base_url": upstream.URL + "/v1",
		},
	}
	if _, errRegister := server.handlers.AuthManager.Register(t.Context(), cred); errRegister != nil {
		t.Fatalf("register Meta credential: %v", errRegister)
	}

	unauth := httptest.NewRecorder()
	server.engine.ServeHTTP(unauth, httptest.NewRequest(http.MethodGet, "/muse-code/models", nil))
	if unauth.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status = %d, want 401", unauth.Code)
	}

	req := httptest.NewRequest(http.MethodGet, "/muse-code/models", nil)
	req.Header.Set("Authorization", "Bearer test-key")
	rec := httptest.NewRecorder()
	server.engine.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	if catalogCalls.Load() != 1 {
		t.Fatalf("Meta catalog calls = %d, want 1", catalogCalls.Load())
	}
	if got := ojson.GetBytes(rec.Body.Bytes(), "data.0.id").String(); got != "muse-wonder-1.0" {
		t.Fatalf("data.0.id = %q, want Meta's muse-wonder-1.0; body=%s", got, rec.Body.String())
	}
}
