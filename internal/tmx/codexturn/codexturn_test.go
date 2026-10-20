package codexturn

import (
	"net/http"
	"testing"
	"time"
)

func TestReplayOnlyOnIssuingAuth(t *testing.T) {
	resetForTest()
	up := http.Header{}
	up.Set("x-codex-turn-state", "ts-1")
	Remember(up, "auth-a")

	client := http.Header{}
	client.Set(Header, "ts-1")

	dst := http.Header{}
	ApplyUpstream(dst, client, "auth-a")
	if got := dst.Get(Header); got != "ts-1" {
		t.Fatalf("same auth: got %q, want ts-1", got)
	}

	dst = http.Header{}
	ApplyUpstream(dst, client, "auth-b")
	if got := dst.Get(Header); got != "" {
		t.Fatalf("other auth: got %q, want empty", got)
	}
}

func TestUnknownOrExpiredTokenIsDropped(t *testing.T) {
	resetForTest()
	client := http.Header{}
	client.Set(Header, "never-issued")
	dst := http.Header{"x-codex-turn-state": {"stale"}}
	ApplyUpstream(dst, client, "auth-a")
	if len(dst) != 0 {
		t.Fatalf("unknown token must be dropped, got %v", dst)
	}

	base := time.Unix(1_800_000_000, 0)
	nowFunc = func() time.Time { return base }
	defer func() { nowFunc = time.Now }()
	up := http.Header{}
	up.Set(Header, "ts-old")
	Remember(up, "auth-a")
	nowFunc = func() time.Time { return base.Add(ttl + time.Minute) }
	if Allowed("ts-old", "auth-a") {
		t.Fatal("expired token must not be allowed")
	}
}

func TestClientHeaders(t *testing.T) {
	if ClientHeaders(http.Header{"Content-Type": {"text/event-stream"}}) != nil {
		t.Fatal("expected nil without turn state")
	}
	up := http.Header{}
	up.Set(Header, "ts-9")
	got := ClientHeaders(up)
	if got.Get(Header) != "ts-9" || len(got) != 1 {
		t.Fatalf("got %v", got)
	}
}
