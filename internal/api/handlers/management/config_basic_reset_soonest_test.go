package management

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
)

func TestNormalizeRoutingStrategyResetSoonest(t *testing.T) {
	for _, input := range []string{"reset-soonest", "resetsoonest", "soonest-reset", "rs", " RESET-SOONEST "} {
		got, ok := normalizeRoutingStrategy(input)
		if !ok || got != "reset-soonest" {
			t.Fatalf("normalizeRoutingStrategy(%q) = %q, %v; want reset-soonest, true", input, got, ok)
		}
	}
}

func TestRoutingStrategyResetSoonestPersists(t *testing.T) {
	gin.SetMode(gin.TestMode)
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("port: 8317\napi-keys: [client]\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	h := &Handler{cfg: cfg, configFilePath: path}
	router := gin.New()
	router.PUT("/v0/management/routing/strategy", h.PutRoutingStrategy)
	router.GET("/v0/management/routing/strategy", h.GetRoutingStrategy)
	router.PUT("/v8/management/config/*path", h.ConfigV8)

	request := func(method, url, body string, status int) string {
		t.Helper()
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, httptest.NewRequest(method, url, strings.NewReader(body)))
		if recorder.Code != status {
			t.Fatalf("%s %s: status=%d body=%s", method, url, recorder.Code, recorder.Body.String())
		}
		return recorder.Body.String()
	}

	request(http.MethodPut, "/v0/management/routing/strategy", `{"value":"rs"}`, http.StatusOK)
	if body := request(http.MethodGet, "/v0/management/routing/strategy", "", http.StatusOK); !strings.Contains(body, `"reset-soonest"`) {
		t.Fatalf("GET strategy body = %s, want reset-soonest", body)
	}
	loaded, err := config.LoadConfig(path)
	if err != nil || loaded.Routing.Strategy != "reset-soonest" {
		t.Fatalf("persisted strategy = %q, %v; want reset-soonest", loaded.Routing.Strategy, err)
	}

	request(http.MethodPut, "/v0/management/routing/strategy", `{"value":"round-robin"}`, http.StatusOK)
	request(http.MethodPut, "/v8/management/config/routing/strategy", `"reset-soonest"`, http.StatusOK)
	loaded, err = config.LoadConfig(path)
	if err != nil || loaded.Routing.Strategy != "reset-soonest" {
		t.Fatalf("v8 persisted strategy = %q, %v; want reset-soonest", loaded.Routing.Strategy, err)
	}
}
