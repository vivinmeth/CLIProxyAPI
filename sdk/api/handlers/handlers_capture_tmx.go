package handlers

import (
	"context"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/capture"
)

// tmxWithCaptureSessionHint runs on the handler goroutine. Executors receive only
// the holder, never a reference to Gin for this bridge, even with a background root.
func tmxWithCaptureSessionHint(ctx context.Context, c *gin.Context) context.Context {
	if !capture.Enabled() || c == nil {
		return ctx
	}
	value, _ := c.Get(capture.SessionHintGinKey)
	holder, _ := value.(*capture.SessionHint)
	if holder == nil {
		holder = &capture.SessionHint{}
		c.Set(capture.SessionHintGinKey, holder)
	}
	return capture.WithSessionHint(ctx, holder)
}
