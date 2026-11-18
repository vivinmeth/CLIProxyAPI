package middleware

// tokenmaxx fork: health-check and discovery probes (internal/tmx/probes) are counted instead of
// written as error logs, so they don't push real errors out of the error-logs-max-files slots.

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/interfaces"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/probes"
)

// tmxProbe reports whether the failed request is a probe, and counts it when it is.
func tmxProbe(c *gin.Context, status int, apiErrors []*interfaces.ErrorMessage) bool {
	if c == nil || c.Request == nil {
		return false
	}
	req := c.Request
	if !probes.Is(c.FullPath(), status, tmxHasBody(req), len(apiErrors) > 0) {
		return false
	}
	probes.Record(req.Method, req.URL.Path, c.ClientIP(), req.UserAgent(), status, time.Now())
	return true
}

// tmxHasBody reports whether a request carried a body.
func tmxHasBody(req *http.Request) bool {
	return req != nil && (req.ContentLength > 0 || (req.ContentLength < 0 && req.Body != nil && req.Body != http.NoBody))
}
