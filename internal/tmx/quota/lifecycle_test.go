package quota

import (
	"context"
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
	blocking := blockingLifecycleFetcher{make(chan struct{}), &auth.Auth{ID: "closing-account", Provider: "claude", Metadata: map[string]any{"access_token": "synthetic"}}}
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
	if err := s.db.QueryRow(`SELECT count(*) FROM ` + s.table + ` WHERE auth_id='closing-account' AND source='passive'`).Scan(&count); err != nil || count == 0 {
		t.Fatalf("final passive flush rows=%d err=%v", count, err)
	}
}
