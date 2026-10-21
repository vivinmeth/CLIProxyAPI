package quota

import (
	"context"
	"strconv"
	"testing"
	"time"

	auth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

type blockingLifecycleFetcher struct {
	started chan struct{}
	auth    *auth.Auth
}

func (f blockingLifecycleFetcher) TmxAuths() []*auth.Auth { return []*auth.Auth{f.auth} }
func (f blockingLifecycleFetcher) TmxFetch(ctx context.Context, _ *auth.Auth, _ string, _ map[string]string) (int, []byte, error) {
	select {
	case <-f.started:
	default:
		close(f.started)
	}
	<-ctx.Done()
	return 0, nil, ctx.Err()
}

func TestQuotaCloseCancelsPollAndFlushesFinalPassive(t *testing.T) {
	now := time.Now()
	f := &fakeFetcher{calls: map[string]int{}}
	s := openTestService(t, f, &now)
	observed := now.Add(time.Second)
	// Unique per run: a reused ID finds its earlier rows and is not polled again.
	id := "closing-account-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	blocking := blockingLifecycleFetcher{make(chan struct{}), &auth.Auth{ID: id, Provider: "claude", Metadata: map[string]any{"access_token": "synthetic"}}}
	s.fetch = blocking
	s.loopCtx, s.loopCancel = context.WithCancel(context.Background())
	s.loopDone = make(chan struct{})
	go s.loop()
	<-blocking.started
	// The poll already holds the detached auth pointer, so synchronize its end before mutation.
	s.loopCancel()
	<-s.loopDone
	blocking.auth.Quota = auth.QuotaState{ObservedAt: observed, Signals: map[string]string{"Anthropic-Ratelimit-Unified-5h-Utilization": "0.25"}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.CloseContext(ctx); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := s.db.QueryRow(`SELECT count(*) FROM ` + s.table + ` WHERE auth_id=$1 AND source='passive'`, id).Scan(&count); err != nil || count == 0 {
		t.Fatalf("final passive flush rows=%d err=%v", count, err)
	}
}

func TestShutdownPassivePreservesWindowClocksAndDropsExpired(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	old := now.Add(-time.Hour)
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	claude := &auth.Auth{ID: "shutdown-clocks-claude-" + suffix, Provider: "claude"}
	claude.Quota.ObserveResponseHeadersForProvider("claude", map[string][]string{"Anthropic-Ratelimit-Unified-7d-Utilization": {"0.2"}}, old)
	claude.Quota.ObserveResponseHeadersForProvider("claude", map[string][]string{"Anthropic-Ratelimit-Unified-5h-Utilization": {"0.1"}}, now)
	codex := &auth.Auth{ID: "shutdown-clocks-codex-" + suffix, Provider: "codex"}
	codex.Quota.ObserveResponseHeadersForProvider("codex", map[string][]string{"X-Codex-Primary-Used-Percent": {"20"}, "X-Codex-Primary-Window-Minutes": {"10080"}, "X-Codex-Primary-Reset-After-Seconds": {"7200"}}, old)
	codex.Quota.ObserveResponseHeadersForProvider("codex", map[string][]string{"X-Codex-Secondary-Used-Percent": {"10"}, "X-Codex-Secondary-Window-Minutes": {"300"}, "X-Codex-Secondary-Reset-After-Seconds": {"3600"}}, now)
	expired := &auth.Auth{ID: "shutdown-expired-" + suffix, Provider: "claude"}
	expired.Quota.ObserveResponseHeadersForProvider("claude", map[string][]string{"Anthropic-Ratelimit-Unified-7d-Utilization": {"0.9"}}, now.Add(-7*time.Hour))
	f := &fakeFetcher{calls: map[string]int{}}
	s := openTestService(t, f, &now)
	f.auths = []*auth.Auth{claude, codex, expired}
	s.loopCtx, s.loopCancel = context.WithCancel(context.Background())
	s.loopDone = make(chan struct{})
	close(s.loopDone)
	if err := s.CloseContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{claude.ID, codex.ID} {
		var ts time.Time
		if err := s.db.QueryRow(`SELECT ts FROM `+s.table+` WHERE auth_id=$1 AND "window"='7d' AND source='passive' ORDER BY ts DESC LIMIT 1`, id).Scan(&ts); err != nil || !ts.Equal(old) {
			t.Fatalf("%s weekly clock %v err=%v want=%v", id, ts, err, old)
		}
	}
	var reset time.Time
	if err := s.db.QueryRow(`SELECT resets_at FROM `+s.table+` WHERE auth_id=$1 AND "window"='7d' ORDER BY ts DESC LIMIT 1`, codex.ID).Scan(&reset); err != nil || !reset.Equal(old.Add(2*time.Hour)) {
		t.Fatalf("relative reset moved: %v %v", reset, err)
	}
	var n int
	if err := s.db.QueryRow(`SELECT count(*) FROM `+s.table+` WHERE auth_id=$1`, expired.ID).Scan(&n); err != nil || n != 0 {
		t.Fatalf("expired rows=%d %v", n, err)
	}
}
