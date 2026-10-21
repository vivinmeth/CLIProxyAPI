package quota

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

type blockingPollFetcher struct {
	*fakeFetcher
	started chan struct{}
	release chan struct{}
	calls   atomic.Int32
}

func (f *blockingPollFetcher) TmxFetch(ctx context.Context, a *coreauth.Auth, url string, h map[string]string) (int, []byte, error) {
	if url == claudeUsageURL {
		f.calls.Add(1)
		f.started <- struct{}{}
		select {
		case <-ctx.Done():
			return 0, nil, ctx.Err()
		case <-f.release:
		}
	}
	return f.fakeFetcher.TmxFetch(ctx, a, url, h)
}

func TestQuotaConcurrentPollNowSingleflight(t *testing.T) {
	now := time.Now().UTC()
	f := &fakeFetcher{auths: []*coreauth.Auth{{ID: "singleflight", Provider: "claude"}}, status: 200, usage: usageJSON(1, 2, 3), calls: map[string]int{}}
	s := openTestService(t, f, &now)
	blocking := &blockingPollFetcher{fakeFetcher: f, started: make(chan struct{}, 20), release: make(chan struct{})}
	s.fetch = blocking
	results := make(chan error, 20)
	start := make(chan struct{})
	for i := 0; i < 20; i++ {
		go func() {
			<-start
			acc, _, err := s.PollNow(context.Background(), "singleflight")
			if err == nil && len(acc.Windows) == 0 {
				t.Error("joined poll returned before the reading")
			}
			results <- err
		}()
	}
	close(start)
	<-blocking.started
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := s.PollNow(ctx, "singleflight"); err != context.Canceled {
		t.Fatalf("joined poll cancellation: %v", err)
	}
	close(blocking.release)
	for i := 0; i < 20; i++ {
		if err := <-results; err != nil {
			t.Error(err)
		}
	}
	if n := blocking.calls.Load(); n != 1 {
		t.Fatalf("20 concurrent PollNow made %d usage calls", n)
	}
}

func TestSharedUsageReadJoinsInventoryAndPoll(t *testing.T) {
	auth := &coreauth.Auth{ID: "shared-inventory", Provider: "claude"}
	started, release := make(chan struct{}), make(chan struct{})
	calls := atomic.Int32{}
	fetch := func(ctx context.Context) (int, []byte, error) {
		calls.Add(1)
		close(started)
		<-release
		return 200, []byte(`{"five_hour":{},"cedar_ember":{}}`), nil
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		code, body, err := ReadUsage(context.Background(), auth, fetch)
		if err != nil || code != 200 || len(body) == 0 {
			t.Errorf("leader: %d %s %v", code, body, err)
		}
	}()
	<-started
	// Observe Done only after ReadUsage registered the follower with singleflight.
	// This makes overlap deterministic without sleeping for the provider call.
	joined := &joinedReadContext{Context: context.Background(), seen: make(chan struct{})}
	wg.Add(1)
	go func() {
		defer wg.Done()
		code, body, err := ReadUsage(joined, auth, func(context.Context) (int, []byte, error) { calls.Add(1); return 500, nil, nil })
		if err != nil || code != 200 || len(body) == 0 {
			t.Errorf("inventory follower: %d %s %v", code, body, err)
		}
	}()
	select {
	case <-joined.seen:
	case <-time.After(time.Second):
		close(release)
		wg.Wait()
		t.Fatal("inventory follower did not join active poll")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	code, _, err := ReadUsage(ctx, auth, func(context.Context) (int, []byte, error) { calls.Add(1); return 500, nil, nil })
	if err != context.Canceled || code != 0 {
		close(release)
		wg.Wait()
		t.Fatalf("canceled follower: %d %v", code, err)
	}
	close(release)
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatal("inventory did not share the active quota read")
	}
}

func TestPollJitterSpreadsAccounts(t *testing.T) {
	a, b := pollJitter("account-a", 10*time.Minute), pollJitter("account-b", 10*time.Minute)
	if a == b || a < 0 || b < 0 || a > time.Minute || b > time.Minute {
		t.Fatalf("jitter: %s %s", a, b)
	}
	if a != pollJitter("account-a", 10*time.Minute) {
		t.Fatal("jitter unstable")
	}
}

func TestPartialPassiveReadingKeepsWeeklyClock(t *testing.T) {
	now := time.Now().UTC()
	a := &coreauth.Auth{Provider: "claude"}
	a.Quota.ObserveResponseHeadersForProvider("claude", map[string][]string{"Anthropic-Ratelimit-Unified-7d-Utilization": {"0.2"}}, now.Add(-time.Hour))
	a.Quota.ObserveResponseHeadersForProvider("claude", map[string][]string{"Anthropic-Ratelimit-Unified-5h-Utilization": {"0.1"}}, now)
	for _, r := range passiveReadings(a, providers["claude"], now) {
		if r.Window.Window == "7d" && !r.ObservedAt.Equal(now.Add(-time.Hour)) {
			t.Fatal("5h observation renewed weekly database reading")
		}
	}
}

type joinedReadContext struct {
	context.Context
	seen chan struct{}
	once sync.Once
}

func (c *joinedReadContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.seen) })
	return c.Context.Done()
}

func TestSharedUsageReadSurvivesLeaderCancellation(t *testing.T) {
	a := &coreauth.Auth{ID: "cancel-leader", Provider: "claude"}
	started, release := make(chan struct{}), make(chan struct{})
	leaderCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	leaderDone := make(chan error, 1)
	go func() {
		_, _, err := ReadUsage(leaderCtx, a, func(ctx context.Context) (int, []byte, error) {
			if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) > time.Minute {
				t.Error("shared fetch has no bounded timeout")
			}
			close(started)
			select {
			case <-ctx.Done():
				return 0, nil, ctx.Err()
			case <-release:
				return 200, []byte("fresh"), nil
			}
		})
		leaderDone <- err
	}()
	<-started
	joined := &joinedReadContext{Context: context.Background(), seen: make(chan struct{})}
	followerDone := make(chan error, 1)
	go func() {
		_, body, err := ReadUsage(joined, a, func(context.Context) (int, []byte, error) { t.Error("follower fetched"); return 500, nil, nil })
		if err == nil && string(body) != "fresh" {
			t.Error("wrong reply")
		}
		followerDone <- err
	}()
	<-joined.seen
	cancel()
	if err := <-leaderDone; err != context.Canceled {
		t.Errorf("leader = %v", err)
	}
	close(release)
	if err := <-followerDone; err != nil {
		t.Errorf("leader cancellation failed follower: %v", err)
	}
}

func TestFreshUsageReadNeverJoinsPreRedeemRead(t *testing.T) {
	a := &coreauth.Auth{ID: "fresh-after-redeem", Provider: "codex"}
	started, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		_, _, _ = ReadUsage(context.Background(), a, func(context.Context) (int, []byte, error) {
			close(started)
			<-release
			return 200, []byte("before"), nil
		})
	}()
	<-started
	defer func() { close(release); <-done }()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	code, body, err := ReadUsageFresh(ctx, a, func(context.Context) (int, []byte, error) { return 200, []byte("after"), nil })
	if err != nil || code != 200 || string(body) != "after" {
		t.Fatalf("fresh read = %d %s %v", code, body, err)
	}
}
