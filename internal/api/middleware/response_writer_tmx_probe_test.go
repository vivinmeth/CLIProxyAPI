package middleware

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/logging"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/probes"
)

// Probes (no route, no body, 404/405) are counted, not written as error logs; real errors and
// bodied requests to unknown routes keep their error-log slots.
func TestProbesDoNotTakeErrorLogSlots(t *testing.T) {
	gin.SetMode(gin.TestMode)
	probes.ResetForTest()
	logsDir := t.TempDir()
	router := gin.New()
	router.Use(RequestLoggingMiddleware(logging.NewFileRequestLogger(false, logsDir, "", 10)))
	router.POST("/v1/messages", func(c *gin.Context) {
		c.JSON(http.StatusTooManyRequests, gin.H{"error": "rate limited"})
	})
	send := func(method, path, body string) {
		var req *http.Request
		if body == "" {
			req = httptest.NewRequest(method, path, nil)
		} else {
			req = httptest.NewRequest(method, path, strings.NewReader(body))
		}
		req.Header.Set("User-Agent", "Bun/1.4.4")
		router.ServeHTTP(httptest.NewRecorder(), req)
	}
	for i := 0; i < 3; i++ {
		send(http.MethodHead, "/api/hello", "")
	}
	send(http.MethodHead, "/v1/messages", "") // registered for POST only: no route for HEAD
	send(http.MethodPost, "/v1/messages", `{"model":"x"}`)
	send(http.MethodPost, "/v1/messagez", `{"model":"x"}`)

	entries, err := os.ReadDir(logsDir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "error-") {
			names = append(names, e.Name())
		}
	}
	if len(names) != 2 || !strings.Contains(strings.Join(names, " "), "v1-messages-") || !strings.Contains(strings.Join(names, " "), "v1-messagez-") {
		t.Fatalf("error logs = %v, want the 429 and the bodied unknown route only", names)
	}
	sum := probes.Snapshot()
	if sum.Total != 4 || len(sum.Sources) != 2 {
		t.Fatalf("probes = %+v", sum)
	}
	top := sum.Sources[0]
	if top.Method != http.MethodHead || top.Path != "/api/hello" || top.Count != 3 || top.Status != 404 || top.UserAgent != "Bun/1.4.4" {
		t.Fatalf("top probe = %+v", top)
	}
}

func TestProbeSourcesBounded(t *testing.T) {
	probes.ResetForTest()
	for i := 0; i < 250; i++ {
		probes.Record("HEAD", "/p"+strconv.Itoa(i), "10.0.0.1", "ua", 404, time.Now())
	}
	if s := probes.Snapshot(); s.Total != 250 || len(s.Sources) != 200 || s.Other != 50 {
		t.Fatalf("total=%d sources=%d other=%d", s.Total, len(s.Sources), s.Other)
	}
}
