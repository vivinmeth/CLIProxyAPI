package middleware

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/logging"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/runtime/executor/helps"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/capture"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/api/handlers"
	coresession "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/session"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v8/sdk/config"
)

func TestTMXCaptureSpilledSourceOwnershipAndChunkTiming(t *testing.T) {
	capture.Configure(128, 64, t.TempDir())
	defer capture.Configure(0, 0, "")
	var exchange *capture.Exchange
	capture.SetSink(func(ex *capture.Exchange) { exchange = ex })
	defer capture.SetSink(nil)
	ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
	ctx.Request = httptest.NewRequest("POST", "/v1/messages", nil)
	wrapper := &ResponseWriterWrapper{ResponseWriter: ctx.Writer, requestInfo: &RequestInfo{URL: "/v1/messages", Body: []byte(`{"model":"opus"}`), Timestamp: time.Now()}}
	wrapper.tmxTee(bytes.Repeat([]byte("a"), 100))
	wrapper.tmxTee([]byte("last"))
	wrapper.tmxSubmit(ctx, 200)
	if exchange == nil {
		t.Fatal("no exchange")
	}
	defer exchange.Release()
	if !exchange.Bodies["response"].Spilled() {
		t.Fatal("response not spooled")
	}
	reader, err := exchange.Bodies["response"].Reader()
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(reader)
	if err != nil || len(got) != 104 {
		t.Fatalf("body length=%d err=%v", len(got), err)
	}
	reader, err = exchange.Bodies["chunks"].Reader()
	if err != nil {
		t.Fatal(err)
	}
	marks, _ := io.ReadAll(reader)
	if !strings.Contains(string(marks), `"n":100`) || !strings.Contains(string(marks), `"n":4`) {
		t.Fatalf("chunk marks %s", marks)
	}
}

// Run each mode in a fresh process with TMX_CAPTURE_RSS_MODE=legacy|bounded.
// Legacy reproduces the pre-fix bytes.Buffer + finalization clone exactly.
func TestTMXCaptureRSS50Concurrent20MiB(t *testing.T) {
	mode := os.Getenv("TMX_CAPTURE_RSS_MODE")
	if mode == "" {
		t.Skip("set TMX_CAPTURE_RSS_MODE=legacy or bounded for the 1 GiB synthetic stream test")
	}
	if mode != "legacy" && mode != "bounded" {
		t.Fatal("invalid mode")
	}
	capture.Configure(0, 0, t.TempDir())
	defer capture.Configure(0, 0, "")
	var mu sync.Mutex
	exchanges := make([]*capture.Exchange, 0, 50)
	capture.SetSink(func(ex *capture.Exchange) { mu.Lock(); exchanges = append(exchanges, ex); mu.Unlock() })
	defer capture.SetSink(nil)
	payload := bytes.Repeat([]byte("x"), 64<<10)
	var ready, done sync.WaitGroup
	ready.Add(50)
	done.Add(50)
	release := make(chan struct{})
	originals := make([]*bytes.Buffer, 50)
	clones := make([][]byte, 50)
	for i := 0; i < 50; i++ {
		go func(index int) {
			defer done.Done()
			if mode == "legacy" {
				body := &bytes.Buffer{}
				originals[index] = body
				for n := 0; n < 120; n++ {
					_, _ = body.Write(payload)
				}
				ready.Done()
				<-release
				for n := 120; n < 320; n++ {
					_, _ = body.Write(payload)
				}
				clones[index] = bytes.Clone(body.Bytes())
			} else {
				w := &ResponseWriterWrapper{requestInfo: &RequestInfo{URL: "/v1/messages", Timestamp: time.Now()}}
				for n := 0; n < 120; n++ {
					w.tmxTee(payload)
				}
				ready.Done()
				<-release
				for n := 120; n < 320; n++ {
					w.tmxTee(payload)
				}
				ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
				ctx.Request = httptest.NewRequest("POST", "/v1/messages", nil)
				w.ResponseWriter = ctx.Writer
				w.tmxSubmit(ctx, 200)
			}
		}(i)
	}
	ready.Wait()
	close(release)
	done.Wait()
	status, err := os.ReadFile("/proc/self/status")
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(status), "\n") {
		if strings.HasPrefix(line, "VmHWM:") || strings.HasPrefix(line, "VmRSS:") {
			t.Logf("%s %s", mode, line)
		}
	}
	runtime.KeepAlive(originals)
	runtime.KeepAlive(clones)
	for _, ex := range exchanges {
		ex.Release()
	}
	if mode == "bounded" && capture.LiveBytes() != 0 {
		t.Fatalf("capture leaked %d bytes", capture.LiveBytes())
	}
}

func TestTMXCaptureDiskFailureNeverInterruptsClient(t *testing.T) {
	path := t.TempDir() + "/not-a-directory"
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	capture.Configure(16, 16, path)
	defer capture.Configure(0, 0, "")
	submitted := false
	capture.SetSink(func(ex *capture.Exchange) { submitted = true; ex.Release() })
	defer capture.SetSink(nil)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest("POST", "/v1/messages", nil)
	wrapper := NewResponseWriterWrapper(ctx.Writer, nil, &RequestInfo{URL: "/v1/messages", Timestamp: time.Now()})
	body := bytes.Repeat([]byte("x"), 1024)
	n, err := wrapper.Write(body)
	if err != nil || n != len(body) || !bytes.Equal(recorder.Body.Bytes(), body) {
		t.Fatalf("client write=%d err=%v", n, err)
	}
	wrapper.tmxSubmit(ctx, 200)
	if submitted {
		t.Fatal("failed capture submitted")
	}
}

func TestTMXSpilledRequestPreservesTrailingSessionIdentity(t *testing.T) {
	capture.Configure(1024, 512, t.TempDir())
	defer capture.Configure(0, 0, "")
	var exchange *capture.Exchange
	capture.SetSink(func(ex *capture.Exchange) { exchange = ex })
	defer capture.SetSink(nil)
	ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
	ctx.Request = httptest.NewRequest("POST", "/v1/messages", nil)
	body := []byte(`{"messages":[{"role":"user","content":"` + strings.Repeat("x", 2<<20) + `"}],"metadata":{"user_id":"{\"session_id\":\"12345678-1234-4123-8123-123456789abc\"}"},"model":"opus","stream":true}`)
	w := NewResponseWriterWrapper(ctx.Writer, nil, &RequestInfo{URL: "/v1/messages", Body: body, Timestamp: time.Now()})
	w.tmxSubmit(ctx, 400)
	if exchange == nil {
		t.Fatal("missing exchange")
	}
	defer exchange.Release()
	if !exchange.Bodies["request"].Spilled() || exchange.ClientSessionID != "12345678-1234-4123-8123-123456789abc" || exchange.SessionID == "" || exchange.RequestModel != "opus" || !exchange.RequestStreaming {
		t.Fatalf("spooled request metadata lost: %+v", exchange)
	}
}

func TestTMXCaptureHandlerPanicReleasesMemoryAndSpill(t *testing.T) {
	for _, threshold := range []int64{1, 1024} {
		t.Run(fmt.Sprint(threshold), func(t *testing.T) {
			dir := t.TempDir()
			capture.Configure(4096, threshold, dir)
			defer capture.Configure(0, 0, "")
			capture.SetSink(func(ex *capture.Exchange) { ex.Release(); t.Error("panic handler submitted capture") })
			defer capture.SetSink(nil)
			before := capture.LiveBytes()
			r := gin.New()
			r.Use(gin.CustomRecovery(func(c *gin.Context, _ any) { c.AbortWithStatus(500) }), RequestLoggingMiddleware(&spyRequestLogger{}))
			r.POST("/v1/messages", func(c *gin.Context) { _, _ = c.Writer.Write([]byte("partial response")); panic("synthetic panic") })
			rec := httptest.NewRecorder()
			r.ServeHTTP(rec, httptest.NewRequest("POST", "/v1/messages", strings.NewReader(`{"model":"opus"}`)))
			if capture.LiveBytes() != before {
				t.Fatalf("leaked bytes=%d", capture.LiveBytes()-before)
			}
			files, _ := filepath.Glob(filepath.Join(dir, "live", "capture-*"))
			if len(files) != 0 {
				t.Fatalf("spill leaked: %v", files)
			}
		})
	}
}

func TestTMXCaptureLateErrorAfterKeepAlive(t *testing.T) {
	capture.Configure(4096, 1024, t.TempDir())
	defer capture.Configure(0, 0, "")
	var exchange *capture.Exchange
	capture.SetSink(func(ex *capture.Exchange) { exchange = ex })
	defer capture.SetSink(nil)
	r := gin.New()
	r.Use(RequestLoggingMiddleware(&spyRequestLogger{}))
	r.POST("/v1/messages", func(c *gin.Context) {
		_, _ = c.Writer.Write([]byte(" "))
		c.Writer.Flush()
		c.Writer.WriteHeader(http.StatusBadGateway)
		_, _ = c.Writer.Write([]byte("upstream failed"))
		wrapper := c.Writer.(*ResponseWriterWrapper)
		if !wrapper.panicked || wrapper.tmxHandlerPanicked {
			t.Fatal("late error must set upstream's error flag without recording a handler panic")
		}
	})
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest("POST", "/v1/messages", strings.NewReader(`{"model":"opus"}`)))
	if exchange == nil {
		t.Fatal("late error after keep-alive was not captured")
	}
	defer exchange.Release()
	reader, err := exchange.Bodies["response"].Reader()
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(reader)
	if err != nil || string(got) != " upstream failed" {
		t.Fatalf("captured body=%q, err=%v", got, err)
	}
}

func TestTMXCaptureSessionHintBoundary(t *testing.T) {
	capture.Configure(1<<20, 1<<16, t.TempDir())
	defer capture.Configure(0, 0, "")
	for _, action := range []string{"generateContent", "streamGenerateContent"} {
		for _, status := range []int{200, 403} {
			for _, logOn := range []bool{false, true} {
				for _, identity := range []string{"inferred", "header", "body", "header-conflict", "body-conflict", "inbound", "conflict", "non-gemini", "countTokens"} {
					t.Run(fmt.Sprintf("%s/%d/log%t/%s", action, status, logOn, identity), func(t *testing.T) {
						var ex *capture.Exchange
						capture.SetSink(func(v *capture.Exchange) { ex = v })
						defer capture.SetSink(nil)
						path := "/v1beta/models/gemini-test:" + action
						body := `{"contents":[{"parts":[{"text":"synthetic"}]}]}`
						want := coresession.NormalizeToCanonicalUUID("header:synthetic-executor")
						switch identity {
						case "non-gemini":
							path = "/v1/messages"
							want = ""
						case "countTokens":
							path = "/v1beta/models/gemini-test:countTokens"
							want = ""
						case "header", "body", "header-conflict", "body-conflict":
							want = coresession.NormalizeToCanonicalUUID("11111111-2222-4333-8444-555555555555")
						case "inbound":
							want = "inbound-authority"
						case "conflict":
							want = ""
						}
						if strings.HasPrefix(identity, "body") {
							body = `{"contents":[],"metadata":{"user_id":"{\"session_id\":\"11111111-2222-4333-8444-555555555555\"}"}}`
						}
						h := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{RequestLog: logOn}, nil)
						r := gin.New()
						r.Use(RequestLoggingMiddleware(logging.NewFileRequestLogger(logOn, t.TempDir(), "", 0)))
						r.POST(path, func(c *gin.Context) {
							if identity == "inbound" {
								c.Request = c.Request.WithContext(logging.WithClientRequestMetadata(c.Request.Context(), logging.ClientRequestMetadata{SessionID: want}))
							}
							ctx, cancel := h.GetContextWithCancel(nil, c, context.Background())
							defer cancel()
							ctx = logging.WithClientRequestMetadata(ctx, logging.ClientRequestMetadata{SessionID: " header:synthetic-executor "})
							helps.RecordAPIRequest(ctx, nil, helps.UpstreamRequestLog{})
							if strings.Contains(identity, "conflict") {
								helps.RecordAPIRequest(logging.WithClientRequestMetadata(ctx, logging.ClientRequestMetadata{SessionID: "header:conflict"}), nil, helps.UpstreamRequestLog{})
							}
							_, _ = io.ReadAll(c.Request.Body)
							c.String(status, "synthetic response")
						})
						req := httptest.NewRequest("POST", path, strings.NewReader(body))
						if strings.HasPrefix(identity, "header") {
							req.Header.Set("X-Session-ID", "11111111-2222-4333-8444-555555555555")
						}
						rec := httptest.NewRecorder()
						r.ServeHTTP(rec, req)
						if ex == nil {
							t.Fatal("no capture")
						}
						defer ex.Release()
						if ex.SessionID != want {
							t.Fatalf("session=%q want=%q", ex.SessionID, want)
						}
						clientWant := ""
						if strings.HasPrefix(identity, "body") {
							clientWant = "11111111-2222-4333-8444-555555555555"
						}
						if ex.ClientSessionID != clientWant {
							t.Fatalf("client session=%q want=%q", ex.ClientSessionID, clientWant)
						}
					})
				}
			}
		}
	}
}

func TestTMXCaptureSessionHintLateAndReusedRequest(t *testing.T) {
	capture.Configure(4096, 1024, t.TempDir())
	defer capture.Configure(0, 0, "")
	var exchanges []*capture.Exchange
	capture.SetSink(func(ex *capture.Exchange) { exchanges = append(exchanges, ex) })
	defer capture.SetSink(nil)
	h := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, nil)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("POST", "/v1beta/models/gemini-test:generateContent", nil)
	oldCtx, cancel := h.GetContextWithCancel(nil, c, context.Background())
	defer cancel()
	old := capture.SessionHintFrom(oldCtx)
	old.Note("old")
	w := NewResponseWriterWrapper(c.Writer, nil, &RequestInfo{URL: c.Request.URL.Path, Timestamp: time.Now()})
	w.tmxSubmit(c, 200)
	old.Note("late")
	// Reuse the same Gin object with new request state, as Gin's pool does.
	c.Keys = nil
	c.Request = httptest.NewRequest("POST", "/v1beta/models/gemini-test:generateContent", nil)
	newCtx, newCancel := h.GetContextWithCancel(nil, c, context.Background())
	defer newCancel()
	fresh := capture.SessionHintFrom(newCtx)
	if fresh == old {
		t.Fatal("reused request retained closed holder")
	}
	old.Note("cross-request")
	fresh.Note("new")
	NewResponseWriterWrapper(c.Writer, nil, &RequestInfo{URL: c.Request.URL.Path, Timestamp: time.Now()}).tmxSubmit(c, 200)
	if len(exchanges) != 2 {
		t.Fatal("missing capture")
	}
	defer exchanges[0].Release()
	defer exchanges[1].Release()
	if exchanges[0].SessionID != "old" || exchanges[1].SessionID != "new" || old.Take() != "" {
		t.Fatal("late write crossed request boundary")
	}
}

func TestTMXCaptureSessionHintEarlyReturnsClose(t *testing.T) {
	for _, mode := range []string{"off", "no-info", "skip", "panic", "body-error"} {
		t.Run(mode, func(t *testing.T) {
			capture.SetSink(func(ex *capture.Exchange) { ex.Release(); t.Error("unexpected capture") })
			defer capture.SetSink(nil)
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest("POST", "/", nil)
			holder := &capture.SessionHint{}
			holder.Note("before")
			c.Set(capture.SessionHintGinKey, holder)
			w := NewResponseWriterWrapper(c.Writer, nil, &RequestInfo{URL: "/"})
			switch mode {
			case "off":
				capture.SetSink(nil)
			case "no-info":
				w.requestInfo = nil
			case "skip":
				c.Set(tmxSkipCaptureKey, true)
			case "panic":
				w.tmxHandlerPanicked = true
			case "body-error":
				path := t.TempDir() + "/file"
				if err := os.WriteFile(path, nil, 0600); err != nil {
					t.Fatal(err)
				}
				capture.Configure(16, 1, path)
				defer capture.Configure(0, 0, "")
				w.tmxTee(bytes.Repeat([]byte("x"), 100))
				if w.tmx == nil || w.tmx.body.Err() == nil {
					t.Fatal("missing synthetic body failure")
				}
			}
			w.tmxSubmit(c, 200)
			holder.Note("late")
			if holder.Take() != "" {
				t.Fatal("early return left holder open")
			}
		})
	}
}

func TestTMXCaptureSessionHintParallelRequests(t *testing.T) {
	capture.Configure(1<<20, 1<<16, t.TempDir())
	defer capture.Configure(0, 0, "")
	var mu sync.Mutex
	got := map[string]string{}
	capture.SetSink(func(ex *capture.Exchange) { defer ex.Release(); mu.Lock(); got[ex.TraceID] = ex.SessionID; mu.Unlock() })
	defer capture.SetSink(nil)
	h := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, nil)
	r := gin.New()
	r.Use(func(c *gin.Context) { logging.SetGinRequestID(c, c.Request.Header.Get("X-Test-Trace")); c.Next() }, RequestLoggingMiddleware(logging.NewFileRequestLogger(false, t.TempDir(), "", 0)))
	r.POST("/v1beta/models/gemini-test:generateContent", func(c *gin.Context) {
		ctx, cancel := h.GetContextWithCancel(nil, c, context.Background())
		defer cancel()
		ctx = logging.WithClientRequestMetadata(ctx, logging.ClientRequestMetadata{SessionID: "header:" + c.Request.Header.Get("X-Test-Trace")})
		helps.RecordAPIRequest(ctx, nil, helps.UpstreamRequestLog{})
		_, _ = io.ReadAll(c.Request.Body)
		c.String(200, "synthetic")
	})
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			req := httptest.NewRequest("POST", "/v1beta/models/gemini-test:generateContent", strings.NewReader(`{"contents":[]}`))
			req.Header.Set("X-Test-Trace", fmt.Sprint(i))
			r.ServeHTTP(httptest.NewRecorder(), req)
		}(i)
	}
	wg.Wait()
	if len(got) != 64 {
		t.Fatalf("captured %d requests", len(got))
	}
	for trace, id := range got {
		if id != coresession.NormalizeToCanonicalUUID("header:"+trace) {
			t.Fatalf("request %s inherited another identity", trace)
		}
	}
}
