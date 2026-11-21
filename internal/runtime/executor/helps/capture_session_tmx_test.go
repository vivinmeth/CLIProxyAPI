package helps

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/logging"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/capture"
	coresession "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/session"
)

func TestTMXRecordAPIRequestSessionWithoutGin(t *testing.T) {
	// No Gin or capture sink: this metadata bridge is independent of logging policy.
	capture.SetSink(nil)
	for i, cfg := range []*config.Config{nil, {}, {SDKConfig: config.SDKConfig{RequestLog: true}}, {CommercialMode: true}} {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			for _, id := range []string{" header:synthetic-session ", " 11111111-2222-4333-8444-555555555555 ", "   "} {
				h := &capture.SessionHint{}
				ctx := logging.WithClientRequestMetadata(capture.WithSessionHint(context.Background(), h), logging.ClientRequestMetadata{SessionID: id})
				RecordAPIRequest(ctx, cfg, UpstreamRequestLog{})
				if got, want := h.Take(), coresession.NormalizeToCanonicalUUID(strings.TrimSpace(id)); got != want {
					t.Fatalf("got=%q want=%q", got, want)
				}
			}
			RecordAPIRequest(context.Background(), cfg, UpstreamRequestLog{})
		})
	}
}
