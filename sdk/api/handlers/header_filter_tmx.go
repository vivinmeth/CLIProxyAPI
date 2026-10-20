package handlers

import (
	"net/http"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/codexturn"
)

// tmxProtocolHeaders adds upstream response headers that clients need to keep their protocol
// working (Codex's x-codex-turn-state) to dst, even when passthrough-headers is off. dst may be
// nil; the result is nil when there is nothing to send.
func tmxProtocolHeaders(upstream http.Header, dst http.Header) http.Header {
	extra := codexturn.ClientHeaders(upstream)
	if len(extra) == 0 {
		return dst
	}
	if dst == nil {
		dst = make(http.Header, len(extra))
	}
	for key, values := range extra {
		if dst.Get(key) == "" {
			dst[key] = values
		}
	}
	return dst
}
