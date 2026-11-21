package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/api/middleware"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/logging"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/runtime/executor/helps"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/capture"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/api/handlers"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/api/handlers/gemini"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	coresession "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/session"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/usage"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v8/sdk/config"
)

type tmxSessionUsagePlugin struct{ records chan usage.Record }

func (p tmxSessionUsagePlugin) HandleUsage(_ context.Context, r usage.Record) {
	if p.records != nil {
		p.records <- r
	}
}

type tmxSessionExecutor struct {
	status int
	cfg    *config.Config
}
type tmxSessionStatusError struct{}

func (tmxSessionStatusError) Error() string    { return "synthetic refusal" }
func (tmxSessionStatusError) StatusCode() int  { return 403 }
func (*tmxSessionExecutor) Identifier() string { return "tmx-session-synthetic" }
func (e *tmxSessionExecutor) record(ctx context.Context, a *coreauth.Auth, req coreexecutor.Request) error {
	// The real manager supplies session metadata, and the actual usage reporter publishes it.
	helps.RecordAPIRequest(ctx, e.cfg, helps.UpstreamRequestLog{Method: "POST", URL: "https://offline.invalid/synthetic"})
	reporter := helps.NewUsageReporter(ctx, e.Identifier(), req.Model, a)
	if e.status == 403 {
		err := tmxSessionStatusError{}
		reporter.PublishFailure(ctx, err)
		return err
	}
	reporter.Publish(ctx, usage.Detail{InputTokens: 1, OutputTokens: 1, TotalTokens: 2})
	return nil
}
func (e *tmxSessionExecutor) Execute(ctx context.Context, a *coreauth.Auth, req coreexecutor.Request, _ coreexecutor.Options) (coreexecutor.Response, error) {
	if err := e.record(ctx, a, req); err != nil {
		return coreexecutor.Response{}, err
	}
	return coreexecutor.Response{Payload: []byte(`{"candidates":[{"content":{"role":"model","parts":[{"text":"synthetic reply"}]}}]}`)}, nil
}
func (e *tmxSessionExecutor) ExecuteStream(ctx context.Context, a *coreauth.Auth, req coreexecutor.Request, _ coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	if err := e.record(ctx, a, req); err != nil {
		return nil, err
	}
	ch := make(chan coreexecutor.StreamChunk, 1)
	ch <- coreexecutor.StreamChunk{Payload: []byte(`{"candidates":[{"content":{"role":"model","parts":[{"text":"synthetic reply"}]}}]}`)}
	close(ch)
	return &coreexecutor.StreamResult{Chunks: ch}, nil
}
func (*tmxSessionExecutor) Refresh(_ context.Context, a *coreauth.Auth) (*coreauth.Auth, error) {
	return a, nil
}
func (*tmxSessionExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("unexpected countTokens")
}
func (*tmxSessionExecutor) HttpRequest(context.Context, *coreauth.Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("no live transport")
}

func TestTMXGeminiHandlerManagerCaptureSession(t *testing.T) {
	// Usage exposes registration but no unregister operation. A fresh test
	// process keeps its manager, registry, Gin mode and capture globals isolated
	// from the rest of this package, including on failures.
	if os.Getenv("TMX_SESSION_TEST_CHILD") != "1" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestTMXGeminiHandlerManagerCaptureSession$", "-test.v")
		cmd.Env = []string{"TMX_SESSION_TEST_CHILD=1", "GOMAXPROCS=4"}
		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("isolated handler/manager boundary: %v\n%s", err, output)
		}
		t.Logf("%s", output)
		return
	}
	gin.SetMode(gin.TestMode)
	capture.Configure(1<<20, 1<<16, t.TempDir())
	defer capture.Configure(0, 0, "")
	records := make(chan usage.Record, 16)
	usage.RegisterNamedPlugin("tmx-session-boundary-test", tmxSessionUsagePlugin{records})
	defer usage.RegisterNamedPlugin("tmx-session-boundary-test", tmxSessionUsagePlugin{})
	for _, action := range []string{"generateContent", "streamGenerateContent"} {
		for _, status := range []int{200, 403} {
			for _, logOn := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%d/log%t", action, status, logOn), func(t *testing.T) {
					var ex *capture.Exchange
					capture.SetSink(func(v *capture.Exchange) { ex = v })
					defer capture.SetSink(nil)
					manager := coreauth.NewManager(nil, nil, nil)
					executor := &tmxSessionExecutor{status: status, cfg: &config.Config{SDKConfig: config.SDKConfig{RequestLog: logOn}}}
					manager.RegisterExecutor(executor)
					const model = "gemini-tmx-session-test"
					a := &coreauth.Auth{ID: "tmx-session-test", Provider: executor.Identifier(), Status: coreauth.StatusActive}
					if _, err := manager.Register(t.Context(), a); err != nil {
						t.Fatal(err)
					}
					registry.GetGlobalRegistry().RegisterClient(a.ID, a.Provider, []*registry.ModelInfo{{ID: model}})
					defer registry.GetGlobalRegistry().UnregisterClient(a.ID)
					h := gemini.NewGeminiAPIHandler(handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager))
					r := gin.New()
					r.Use(func(c *gin.Context) {
						logging.SetGinRequestID(c, "tmx-session-trace")
						c.Request = c.Request.WithContext(logging.WithRequestID(c.Request.Context(), "tmx-session-trace"))
						c.Next()
					}, middleware.RequestLoggingMiddleware(logging.NewFileRequestLogger(logOn, t.TempDir(), "", 0)))
					r.POST("/v1beta/models/*action", h.GeminiHandler)
					req := httptest.NewRequest("POST", "/v1beta/models/"+model+":"+action+"?alt=sse", strings.NewReader(`{"contents":[{"role":"user","parts":[{"text":"synthetic session boundary"}]}]}`))
					req.Header.Set("Content-Type", "application/json")
					rec := httptest.NewRecorder()
					r.ServeHTTP(rec, req)
					if rec.Code != status {
						t.Fatalf("status=%d want=%d body=%s", rec.Code, status, rec.Body.String())
					}
					if ex == nil {
						t.Fatal("no capture")
					}
					defer ex.Release()
					select {
					case record := <-records:
						normalized := coresession.NormalizeToCanonicalUUID(strings.TrimSpace(record.SessionID))
						if normalized == "" {
							t.Fatal("real manager/usage session missing")
						}
						if ex.TraceID == "" || ex.TraceID != record.TraceID {
							t.Fatalf("trace mismatch capture=%q usage=%q", ex.TraceID, record.TraceID)
						}
						if ex.SessionID != normalized {
							t.Fatalf("capture lost manager identity: captured=%q normalized usage=%q", ex.SessionID, normalized)
						}
						if ex.ClientSessionID != "" {
							t.Fatal("inferred identity became client-supplied identity")
						}
					case <-time.After(5 * time.Second):
						t.Fatal("usage plugin did not receive record")
					}
				})
			}
		}
	}
}
