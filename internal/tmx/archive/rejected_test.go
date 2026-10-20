package archive

import (
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/capture"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/requestlog"
)

func TestRejectedRow(t *testing.T) {
	start := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
	base := func() *capture.Exchange {
		return &capture.Exchange{TraceID: "t1", URL: "/v1/messages?beta=true", Method: "POST", StartedAt: start,
			FinishedAt: start.Add(3 * time.Millisecond), Status: 400,
			RequestHeaders: map[string][]string{"X-Api-Key": {"sk-tmx-macbook-abc"},
				"User-Agent": {"claude-cli/2.1.304 (external, cli)"}, "X-Forwarded-For": {"10.0.0.5, 10.0.1.1"}},
			RequestBody:  []byte(`{"model":"claude-nope-9","stream":true}`),
			ResponseBody: []byte(`{"error":{"message":"unknown provider for model claude-nope-9"}}`)}
	}
	row, ok := rejectedRow(base())
	if !ok || row.Outcome != requestlog.OutcomeRejected || row.StatusCode != 400 || row.Model != "claude-nope-9" ||
		row.Machine != "macbook" || row.APIKeyHash != requestlog.HashAPIKey("sk-tmx-macbook-abc") ||
		row.ClientIP != "10.0.0.5" || !row.Stream || row.LatencyMs != 3 || row.Error == "" {
		t.Fatalf("row = %+v ok=%v", row, ok)
	}
	bearer := base()
	bearer.RequestHeaders = map[string][]string{"Authorization": {"Bearer sk-tmx-alien-x"}}
	if row, _ = rejectedRow(bearer); row.Machine != "alien" {
		t.Fatalf("bearer key machine = %q", row.Machine)
	}
	for name, mutate := range map[string]func(*capture.Exchange){
		"success":            func(e *capture.Exchange) { e.Status = 200 },
		"reached upstream":   func(e *capture.Exchange) { e.APIRequest = []byte("=== API REQUEST 1 ===") },
		"websocket upstream": func(e *capture.Exchange) { e.APIWebsocketTimeline = []byte("x") },
		"probe":              func(e *capture.Exchange) { e.Method, e.URL, e.Status = "HEAD", "/api/hello", 404 },
		"management":         func(e *capture.Exchange) { e.URL = "/v8/management/config" },
		"models list":        func(e *capture.Exchange) { e.Method, e.URL = "GET", "/v1/models" },
	} {
		ex := base()
		mutate(ex)
		if _, ok = rejectedRow(ex); ok {
			t.Errorf("%s: recorded as rejected", name)
		}
	}
	ws := base()
	ws.Method, ws.URL, ws.Status = "GET", "/v1/responses", 429
	ws.RequestHeaders = map[string][]string{"Upgrade": {"websocket"}}
	if _, ok = rejectedRow(ws); !ok {
		t.Fatal("refused websocket connection not recorded")
	}
}
