package handlers

// tokenmaxx fork: an error's computed Retry-After (the later of the quota deadlines, rounded and
// capped for clients) is authoritative. A raw upstream Retry-After copied by passthrough-headers
// or into a WebSocket error envelope must not replace it.

import (
	"net/http"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/interfaces"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

// ErrorResponseHeaders returns the headers an error response carries: the upstream ones (minus
// CPA-reserved headers) when upstream is true, then the computed Retry-After over any raw one.
func ErrorResponseHeaders(msg *interfaces.ErrorMessage, upstream bool) http.Header {
	out := http.Header{}
	if msg == nil {
		return out
	}
	if upstream {
		for key, values := range msg.Addon {
			if len(values) == 0 || IsCPAReservedResponseHeader(key) {
				continue
			}
			out[http.CanonicalHeaderKey(key)] = append([]string(nil), values...)
		}
	}
	if msg.Error != nil {
		if computed := coreauth.SafeResponseHeaders(msg.Error).Values("Retry-After"); len(computed) > 0 {
			out["Retry-After"] = computed
		}
	}
	return out
}

// WriteErrorResponseHeaders writes ErrorResponseHeaders, honouring passthrough-headers.
func (h *BaseAPIHandler) WriteErrorResponseHeaders(dst http.Header, msg *interfaces.ErrorMessage) {
	for key, values := range ErrorResponseHeaders(msg, PassthroughHeadersEnabled(h.CurrentConfig())) {
		dst[key] = values
	}
}

// ErrorEnvelopeHeaders is msg.Addon with the computed Retry-After over a raw one, for error
// envelopes that always carry the upstream headers (Responses WebSocket).
func ErrorEnvelopeHeaders(msg *interfaces.ErrorMessage) http.Header {
	if msg == nil || msg.Addon == nil {
		return nil
	}
	out := http.Header{}
	for key, values := range msg.Addon {
		out[http.CanonicalHeaderKey(key)] = values
	}
	if msg.Error != nil {
		if computed := coreauth.SafeResponseHeaders(msg.Error).Values("Retry-After"); len(computed) > 0 && out.Get("Retry-After") != "" {
			out["Retry-After"] = computed
		}
	}
	return out
}
