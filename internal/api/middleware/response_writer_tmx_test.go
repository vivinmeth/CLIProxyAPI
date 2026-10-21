package middleware

import (
	"bytes"
	"fmt"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/capture"
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
