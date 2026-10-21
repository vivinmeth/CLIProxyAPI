package archive

import (
	"net/http"
	"net/url"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/capture"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/requestlog"
	"github.com/tidwall/gjson"
)

const rejectedErrorMax = 2000

// modelPathPrefixes are the client API surfaces; anything else (management, probes such as
// HEAD /api/hello, static assets) is not a model request and is never recorded as rejected.
var modelPathPrefixes = []string{"/v1/", "/v1beta/", "/v1beta1/", "/api/provider/", "/backend-api/"}

// rejectedRow turns an exchange the proxy answered with an error before trying any account into a
// request-log row (outcome "rejected"). Exchanges that reached an upstream are recorded by the
// usage plugin instead, so they return false here.
func rejectedRow(ex *capture.Exchange) (requestlog.Row, bool) {
	if ex == nil || ex.Status < 400 || ex.BodySize("upstream.request", ex.APIRequest) > 0 || ex.BodySize("upstream.ws", ex.APIWebsocketTimeline) > 0 {
		return requestlog.Row{}, false
	}
	if ex.Method != http.MethodPost && !(ex.Method == http.MethodGet && isWebsocket(ex.RequestHeaders)) {
		return requestlog.Row{}, false
	}
	path := ex.URL
	if u, err := url.Parse(ex.URL); err == nil {
		path = u.Path
	}
	if path == "/v1/models" || !hasModelPrefix(path) {
		return requestlog.Row{}, false
	}
	h := http.Header(ex.RequestHeaders)
	key := clientKey(h, ex.URL)
	latency := int64(0)
	if !ex.FinishedAt.IsZero() {
		latency = ex.FinishedAt.Sub(ex.StartedAt).Milliseconds()
	}
	clientIP, _, _ := strings.Cut(h.Get("X-Forwarded-For"), ",")
	request := capturePrefix(ex, "request", ex.RequestBody, 1<<20)
	model := gjson.GetBytes(request, "model").String()
	return requestlog.Row{
		Source:     requestlog.SourceProxy,
		TS:         ex.StartedAt.UTC(),
		RequestID:  "rejected:" + ex.TraceID,
		TraceID:    ex.TraceID,
		SessionID:  ex.SessionID,
		Model:      strings.TrimSpace(model),
		Alias:      strings.TrimSpace(model),
		APIKeyHash: requestlog.HashAPIKey(key),
		Machine:    requestlog.MachineFromKey(key),
		ClientIP:   strings.TrimSpace(clientIP),
		UserAgent:  truncateString(h.Get("User-Agent"), 300),
		Stream:     gjson.GetBytes(request, "stream").Bool(),
		Outcome:    requestlog.OutcomeRejected,
		StatusCode: ex.Status,
		Error:      truncateString(string(capturePrefix(ex, "response", ex.ResponseBody, rejectedErrorMax)), rejectedErrorMax),
		LatencyMs:  latency,
	}, true
}

func hasModelPrefix(path string) bool {
	for _, p := range modelPathPrefixes {
		if strings.HasPrefix(path, p) {
			return true
		}
	}
	return false
}

func isWebsocket(h map[string][]string) bool {
	return strings.EqualFold(http.Header(h).Get("Upgrade"), "websocket")
}

// clientKey finds the client key the request used, as the access middleware would.
func clientKey(h http.Header, rawURL string) string {
	if v := strings.TrimSpace(h.Get("Authorization")); v != "" {
		if k, ok := strings.CutPrefix(v, "Bearer "); ok {
			return strings.TrimSpace(k)
		}
		return v
	}
	for _, name := range []string{"X-Api-Key", "X-Goog-Api-Key", "Api-Key"} {
		if v := strings.TrimSpace(h.Get(name)); v != "" {
			return v
		}
	}
	if u, err := url.Parse(rawURL); err == nil {
		return strings.TrimSpace(u.Query().Get("key"))
	}
	return ""
}

func truncateString(s string, n int) string {
	s = strings.TrimSpace(s)
	if len(s) <= n {
		return s
	}
	for n > 0 && (s[n]&0xC0) == 0x80 {
		n--
	}
	return s[:n] + "…"
}
