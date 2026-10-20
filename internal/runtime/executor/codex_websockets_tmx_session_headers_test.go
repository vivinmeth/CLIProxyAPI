package executor

import (
	"context"
	"net/http"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
)

// tmx: native Codex over WebSocket must reach the ChatGPT backend with the client's own
// Session-Id/Thread-Id/X-Codex-Window-Id (as over HTTP), even with cloaking on. Without them
// every response.create counts as a new turn (turn_rate_limit_exceeded).
func TestCodexWebsocketHandshakeMirrorsNativeSessionHeaders(t *testing.T) {
	client := http.Header{}
	client.Set("Session-Id", "s-1")
	client.Set("Thread-Id", "t-1")
	client.Set("X-Codex-Window-Id", "t-1:0")
	cfg := &config.Config{}

	pre := http.Header{}
	pre.Set("Conversation_id", "s-1")
	setHeaderCasePreserved(pre, "session_id", "s-1")
	got := applyCodexWebsocketHeaders(context.Background(), pre, codexTestAuth("http://x"), "tok", cfg, true, client)
	for key, want := range map[string]string{"Session-Id": "s-1", "Thread-Id": "t-1", "X-Codex-Window-Id": "t-1:0"} {
		if v := got.Get(key); v != want {
			t.Errorf("native %s = %q, want %q (headers %v)", key, v, want, got)
		}
	}
	for _, key := range []string{"session_id", "Conversation_id"} {
		if _, ok := got[key]; ok {
			t.Errorf("native handshake still has %s: %v", key, got)
		}
	}

	pre = http.Header{}
	setHeaderCasePreserved(pre, "session_id", "s-1")
	got = applyCodexWebsocketHeaders(context.Background(), pre, codexTestAuth("http://x"), "tok", cfg, false, client)
	if v := headerValueCaseInsensitive(got, "session_id"); v != "s-1" {
		t.Errorf("non-native session_id = %q, want s-1 (headers %v)", v, got)
	}
}
