package codexturn

import (
	"net/http"
	"strings"
)

// sessionHeaders are the per-thread headers current Codex sends on the WebSocket handshake.
// The ChatGPT backend uses them to group a thread's requests (prewarm, turn, tool-call
// follow-ups) into one turn. Over WebSocket Codex never replays x-codex-turn-state, so without
// them every response.create counts as a new turn and bursts hit turn_rate_limit_exceeded.
var sessionHeaders = []string{"Session-Id", "Thread-Id", "X-Codex-Window-Id"}

// MirrorWebsocketSessionHeaders makes the upstream WebSocket handshake carry the client's own
// session headers, as the HTTP path and a direct Codex login do. Call it for native Codex
// requests only; it acts when the client sent Thread-Id (current Codex) and then drops the
// proxy's underscore session_id and conversation_id, which current Codex does not send.
func MirrorWebsocketSessionHeaders(dst http.Header, client http.Header) {
	if dst == nil || strings.TrimSpace(client.Get("Thread-Id")) == "" {
		return
	}
	for _, key := range []string{"session_id", "session-id", "conversation_id", "thread-id", "x-codex-window-id"} {
		deleteFold(dst, key)
	}
	for _, key := range sessionHeaders {
		if value := strings.TrimSpace(client.Get(key)); value != "" {
			dst.Set(key, value)
		}
	}
}

func deleteFold(h http.Header, key string) {
	for k := range h {
		if strings.EqualFold(k, key) {
			delete(h, k)
		}
	}
}
