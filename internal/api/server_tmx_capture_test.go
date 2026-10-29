package api

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	proxyconfig "github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/logging"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/capture"
)

// tokenmaxx fork: the archive stores what the handler wrote, not the compressed bytes sent to
// the client; conversation extraction parses the captured body as JSON/SSE.
func TestTMXCaptureSeesUncompressedResponse(t *testing.T) {
	var exchange *capture.Exchange
	capture.SetSink(func(ex *capture.Exchange) { exchange = ex })
	defer capture.SetSink(nil)
	logsDir := t.TempDir()
	server := newTestServerWithConfig(t, &proxyconfig.Config{}, WithRequestLoggerFactory(
		func(*proxyconfig.Config, string) logging.RequestLogger {
			return logging.NewFileRequestLogger(false, logsDir, logsDir, 0)
		}))
	stream := strings.Repeat("event: content_block_delta\ndata: {\"type\":\"content_block_delta\"}\n\n", 40)
	server.engine.POST("/v1/tmx-capture-test", func(c *gin.Context) {
		c.Header("Content-Type", "text/event-stream")
		c.Status(http.StatusOK)
		_, _ = io.WriteString(c.Writer, stream)
	})
	request := httptest.NewRequest(http.MethodPost, "/v1/tmx-capture-test", strings.NewReader(`{"model":"claude"}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept-Encoding", "gzip")
	response := httptest.NewRecorder()
	server.engine.ServeHTTP(response, request)

	if got := response.Header().Get("Content-Encoding"); got != "gzip" {
		t.Fatalf("client Content-Encoding = %q, want gzip", got)
	}
	if got := string(compressionTestBody(t, "gzip", response.Body.Bytes())); got != stream {
		t.Fatal("client body differs")
	}
	if exchange == nil {
		t.Fatal("no exchange captured")
	}
	defer exchange.Release()
	reader, err := exchange.Bodies["response"].Reader()
	if err != nil {
		t.Fatal(err)
	}
	captured, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if string(captured) != stream {
		t.Fatalf("captured %d bytes starting %q, want the plain stream", len(captured), captured[:min(len(captured), 16)])
	}
	if got := http.Header(exchange.ResponseHeaders).Get("Content-Encoding"); got != "" {
		t.Fatalf("archived headers say Content-Encoding %q for a plain body", got)
	}
}
