package helps

import (
	"context"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/logging"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/capture"
	coresession "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/session"
)

// tmxNoteCaptureSession uses the same normalization as requestlog, before logging
// policy gates. It must not look up Gin: execution may outlive its pooled context.
func tmxNoteCaptureSession(ctx context.Context) {
	if holder := capture.SessionHintFrom(ctx); holder != nil {
		holder.Note(coresession.NormalizeToCanonicalUUID(strings.TrimSpace(logging.GetClientRequestMetadata(ctx).SessionID)))
	}
}
