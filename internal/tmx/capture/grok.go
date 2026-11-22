package capture

import (
	"net/http"
	"net/url"
	"strings"

	coresession "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/session"
)

// GrokSessionIdentity preserves the separate native session and conversation group
// supplied by Grok. Helpers can have a group without a native session. Call only
// after existing client identities have had precedence; this is archive metadata,
// not an input to affinity or routing.
func GrokSessionIdentity(method, rawURL, model string, headers http.Header) (native, group string) {
	if method != http.MethodPost || !strings.HasPrefix(model, "grok-") || len(model) == len("grok-") {
		return "", ""
	}
	u, err := url.Parse(rawURL)
	if err != nil || u.Opaque != "" || (u.Scheme != "" && (u.Scheme != "http" && u.Scheme != "https" || u.Host == "")) {
		return "", ""
	}
	if u.Path != "/v1/chat/completions" && u.Path != "/v1/responses" {
		return "", ""
	}
	switch grokHeaderIdentity(headers, "X-Grok-Client-Identifier") {
	case "grok-shell", "grok-code-extension":
	default:
		return "", ""
	}
	return grokHeaderIdentity(headers, "X-Grok-Session-Id"), grokHeaderIdentity(headers, "X-Grok-Conv-Group-Id")
}

// Reject ambiguous duplicate fields instead of depending on map iteration order.
// NormalizeExplicitID bounds opaque IDs and rejects controls without inventing one.
func grokHeaderIdentity(headers http.Header, name string) string {
	var id string
	seen := false
	for key, values := range headers {
		if !strings.EqualFold(key, name) {
			continue
		}
		for _, value := range values {
			normalized := coresession.NormalizeExplicitID(value)
			if normalized == "" || seen && normalized != id {
				return ""
			}
			id, seen = normalized, true
		}
	}
	return id
}
