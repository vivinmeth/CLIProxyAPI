package api

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/httpwire"
)

type failedFlushTransport struct {
	*httptest.ResponseRecorder
	err error
}

func (w failedFlushTransport) FlushError() error { return w.err }

func TestTMXDrainWriterPreservesFlushError(t *testing.T) {
	want := errors.New("client disconnected")
	transport := failedFlushTransport{httptest.NewRecorder(), want}
	c, _ := gin.CreateTestContext(transport)
	w := &tmxDrainWriter{ResponseWriter: c.Writer}
	if err := httpwire.FlushResponse(w); !errors.Is(err, want) {
		t.Fatalf("flush error = %v, want %v", err, want)
	}
	if u, ok := any(w).(interface{ Unwrap() http.ResponseWriter }); !ok || u.Unwrap() != c.Writer {
		t.Fatal("missing transport unwrap")
	}
}
