package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

// The archive routes register without conflicts (gin panics on overlapping paths) and answer 503
// while the archive is off.
func TestTMXArchiveIngestRoutes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	registerTMXArchiveRoutes(r.Group("/v8/management"))
	for _, rt := range []struct{ method, path string }{
		{http.MethodPut, "/v8/management/tmx/archive/prompts"},
		{http.MethodGet, "/v8/management/tmx/archive/prompts"},
		{http.MethodPut, "/v8/management/tmx/archive/attachments"},
		{http.MethodGet, "/v8/management/tmx/archive/attachments"},
		{http.MethodGet, "/v8/management/tmx/archive/files?harness=a&session_id=b&name=c"},
		{http.MethodHead, "/v8/management/tmx/archive/files?harness=a&session_id=b&name=c"},
		{http.MethodPost, "/v8/management/tmx/archive/files/state"},
		{http.MethodPost, "/v8/management/tmx/archive/files/append"},
		{http.MethodGet, "/v8/management/tmx/archive/files/content"},
	} {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(rt.method, rt.path, nil))
		if w.Code != http.StatusServiceUnavailable {
			t.Errorf("%s %s = %d, want 503 (archive off)", rt.method, rt.path, w.Code)
		}
	}
}
