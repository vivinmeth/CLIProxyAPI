package muse_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/runtime/executor"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/muse"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	"github.com/tidwall/gjson"
)

func TestHandlerForwardsMetaCatalog(t *testing.T) {
	const upstream = `{"object":"list","data":[{"id":"muse-real-9","object":"model","owned_by":"meta","metadata":{"muse-code":{"name":"Real"}}}]}`
	var gotAuth, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth, gotPath = r.Header.Get("Authorization"), r.URL.Path
		_, _ = w.Write([]byte(upstream))
	}))
	defer srv.Close()

	mgr := auth.NewManager(nil, nil, nil)
	mgr.RegisterExecutor(executor.NewMetaExecutor(&config.Config{}))
	if _, err := mgr.Register(context.Background(), &auth.Auth{ID: "meta-1", Provider: "meta",
		Attributes: map[string]string{"api_key": "minted-key", "base_url": srv.URL + "/v1"}}); err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	muse.Handler(mgr)(rec, httptest.NewRequest(http.MethodGet, muse.CatalogPath, nil))
	if rec.Body.String() != upstream {
		t.Fatalf("body = %s", rec.Body.String())
	}
	if gotPath != muse.CatalogPath || gotAuth != "Bearer minted-key" {
		t.Fatalf("upstream got path %q auth %q", gotPath, gotAuth)
	}

	// Upstream failure falls back to the registry catalog.
	srv.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusUnauthorized) })
	rec = httptest.NewRecorder()
	muse.Handler(mgr)(rec, httptest.NewRequest(http.MethodGet, muse.CatalogPath, nil))
	if !gjson.GetBytes(rec.Body.Bytes(), `data.#(id=="muse-wonder-full-1.0")`).Exists() {
		t.Fatalf("fallback body = %s", rec.Body.String())
	}
}
