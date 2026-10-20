package codexturn

import (
	"net/http"
	"testing"
)

func TestMirrorWebsocketSessionHeadersForCurrentCodex(t *testing.T) {
	client := http.Header{}
	client.Set("Session-Id", "s-1")
	client.Set("Thread-Id", "t-1")
	client.Set("X-Codex-Window-Id", "t-1:0")

	dst := http.Header{"session_id": {"s-1"}, "Conversation_id": {"s-1"}, "X-Codex-Routing-Hint": {"model=m"}}
	MirrorWebsocketSessionHeaders(dst, client)

	want := map[string]string{"Session-Id": "s-1", "Thread-Id": "t-1", "X-Codex-Window-Id": "t-1:0", "X-Codex-Routing-Hint": "model=m"}
	if len(dst) != len(want) {
		t.Fatalf("got %v, want keys %v", dst, want)
	}
	for k, v := range want {
		if got := dst[k]; len(got) != 1 || got[0] != v {
			t.Fatalf("%s: got %v, want %q (all: %v)", k, got, v, dst)
		}
	}
}

func TestMirrorWebsocketSessionHeadersLeavesOtherClients(t *testing.T) {
	client := http.Header{}
	client.Set("Session_id", "s-1")
	dst := http.Header{"session_id": {"s-1"}, "Conversation_id": {"s-1"}}
	MirrorWebsocketSessionHeaders(dst, client)
	if len(dst) != 2 || dst["session_id"][0] != "s-1" || dst["Conversation_id"][0] != "s-1" {
		t.Fatalf("headers changed for a client without Thread-Id: %v", dst)
	}
	MirrorWebsocketSessionHeaders(dst, nil)
	if len(dst) != 2 {
		t.Fatalf("headers changed with nil client headers: %v", dst)
	}
}
