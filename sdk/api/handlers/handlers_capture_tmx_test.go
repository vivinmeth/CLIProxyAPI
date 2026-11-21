package handlers

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/capture"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v8/sdk/config"
)

func TestTMXGetContextCaptureSessionHint(t *testing.T) {
	capture.SetSink(func(ex *capture.Exchange) { ex.Release() })
	defer capture.SetSink(nil)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("POST", "/v1beta/models/gemini-test:generateContent", nil)
	h := NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, nil)
	ctx, cancel := h.GetContextWithCancel(nil, c, context.Background())
	defer cancel()
	holder := capture.SessionHintFrom(ctx)
	if holder == nil {
		t.Fatal("background root lost holder")
	}
	again, cancelAgain := h.GetContextWithCancel(nil, c, context.Background())
	defer cancelAgain()
	if capture.SessionHintFrom(again) != holder {
		t.Fatal("repeated call replaced request holder")
	}
	value, _ := c.Get(capture.SessionHintGinKey)
	if value != holder {
		t.Fatal("capture cannot find execution holder")
	}
	other, _ := gin.CreateTestContext(httptest.NewRecorder())
	other.Request = httptest.NewRequest("POST", "/", nil)
	otherCtx, otherCancel := h.GetContextWithCancel(nil, other, context.Background())
	defer otherCancel()
	if capture.SessionHintFrom(otherCtx) == holder {
		t.Fatal("requests shared holder")
	}
	capture.SetSink(nil)
	off, _ := gin.CreateTestContext(httptest.NewRecorder())
	off.Request = httptest.NewRequest("POST", "/", nil)
	offCtx, offCancel := h.GetContextWithCancel(nil, off, context.Background())
	defer offCancel()
	if capture.SessionHintFrom(offCtx) != nil {
		t.Fatal("capture off allocated holder")
	}
	if _, exists := off.Get(capture.SessionHintGinKey); exists {
		t.Fatal("capture off stored holder")
	}
}
