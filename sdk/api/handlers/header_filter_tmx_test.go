package handlers

import (
	"net/http"
	"testing"
)

func TestCodexTurnStateReachesClientWithoutPassthrough(t *testing.T) {
	upstream := http.Header{}
	upstream.Set("X-Codex-Turn-State", "ts-1")
	upstream.Set("X-Codex-Plan-Type", "prolite")

	got := downstreamHeadersFromExecutor(upstream, false)
	if got.Get("X-Codex-Turn-State") != "ts-1" {
		t.Fatalf("turn state not forwarded: %v", got)
	}
	if got.Get("X-Codex-Plan-Type") != "" {
		t.Fatalf("other upstream headers must stay hidden without passthrough: %v", got)
	}

	got = downstreamHeadersAfterInterceptors(upstream, upstream, false)
	if got.Get("X-Codex-Turn-State") != "ts-1" || got.Get("X-Codex-Plan-Type") != "" {
		t.Fatalf("stream path: got %v", got)
	}

	if downstreamHeadersFromExecutor(http.Header{"X-Codex-Plan-Type": {"prolite"}}, false) != nil {
		t.Fatal("expected nil without turn state")
	}
}
