package auth

import (
	"context"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

// Publish refresh activity before rejecting a token. Selection may join this activity,
// but must never make the rejected token available for another upstream attempt.
func (l *authRefreshLock) begin() func() {
	l.stateMu.Lock()
	if l.pending == 0 {
		l.done = make(chan struct{})
	}
	l.pending++
	l.stateMu.Unlock()
	return func() {
		l.stateMu.Lock()
		l.pending--
		if l.pending == 0 {
			close(l.done)
			l.done = nil
		}
		l.stateMu.Unlock()
	}
}

// refreshWaitBudget bounds the total time one client request spends waiting on token refreshes:
// the refresh lock and in-flight refreshes of other credentials, across every pick and retry.
var refreshWaitBudget = 30 * time.Second

type refreshWaitBudgetKey struct{}

type refreshWaitSpent struct{ nanos atomic.Int64 }

// withRefreshWaitBudget gives a request its shared wait budget; an existing one is kept.
func withRefreshWaitBudget(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, ok := ctx.Value(refreshWaitBudgetKey{}).(*refreshWaitSpent); ok {
		return ctx
	}
	return context.WithValue(ctx, refreshWaitBudgetKey{}, &refreshWaitSpent{})
}

// refreshWaitContext bounds one wait by what is left of the request's budget (a whole budget
// outside a request) and returns a done func that charges the time waited.
func refreshWaitContext(ctx context.Context) (context.Context, func()) {
	spent, _ := ctx.Value(refreshWaitBudgetKey{}).(*refreshWaitSpent)
	if spent == nil {
		return context.WithTimeout(ctx, refreshWaitBudget)
	}
	start := time.Now()
	waitCtx, cancel := context.WithTimeout(ctx, refreshWaitBudget-time.Duration(spent.nanos.Load()))
	return waitCtx, func() {
		cancel()
		spent.nanos.Add(int64(time.Since(start)))
	}
}

// refreshCanUnblock reports whether a's own token (rejected or expired) is what keeps it from
// serving model, so a refresh in flight may make it pickable. A credential or model cooldown for
// any other reason (quota, overload, transient) outlasts the refresh.
func refreshCanUnblock(a *Auth, model string, now time.Time) bool {
	if exp, ok := a.AccessTokenExpirationTime(); !ok || exp.IsZero() || exp.After(now) {
		return false
	}
	if a.Quota.Exceeded && a.Quota.Reason == "credential_quota" && a.Quota.NextRecoverAt.After(now) {
		return false
	}
	key := canonicalModelKey(model)
	if key == "" {
		return true
	}
	for name, state := range a.ModelStates {
		if state == nil || canonicalModelKey(name) != key {
			continue
		}
		blocked, _, _ := availabilityBlock(state.Unavailable, state.Quota.Exceeded, state.NextRetryAfter, state.Quota.NextRecoverAt, now)
		if blocked && statusCodeFromResult(state.LastError) != 401 {
			return false
		}
	}
	return true
}

func (m *Manager) refreshingCandidates(ctx context.Context, providers []string, model string, opts cliproxyexecutor.Options, tried map[string]struct{}) []<-chan struct{} {
	eligibility := authSelectionEligibilityForRequest(ctx, opts)
	pinned := pinnedAuthIDFromMetadata(opts.Metadata)
	now := time.Now()
	var waits []<-chan struct{}
	m.mu.RLock()
	defer m.mu.RUnlock()
	for id, a := range m.auths {
		if a == nil || a.Disabled || !eligibility.allows(a) || (pinned != "" && pinned != id) {
			continue
		}
		if _, used := tried[id]; used {
			continue
		}
		matches := false
		for _, p := range providers {
			if canonicalSchedulingProvider(executorKeyFromAuth(a)) == canonicalSchedulingProvider(p) {
				matches = true
			}
		}
		if !matches || !refreshCanUnblock(a, model, now) {
			continue
		}
		if value, ok := m.refreshLocks.Load(id); ok {
			l := value.(*authRefreshLock)
			l.stateMu.Lock()
			if l.done != nil {
				waits = append(waits, l.done)
			}
			l.stateMu.Unlock()
		}
	}
	return waits
}

type refreshSelectionContextKey struct{}

// retryDuringRefresh retries a pick that found no usable credential once a refresh that can
// make one usable finishes. When the wait budget runs out the pick's own error (a 429 with its
// Retry-After) stands; only the caller's cancellation is reported instead.
func retryDuringRefresh(m *Manager, ctx context.Context, providers []string, model string, opts cliproxyexecutor.Options, tried map[string]struct{}, pick func(context.Context) (refreshSelection, error)) (refreshSelection, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if ctx.Value(refreshSelectionContextKey{}) != nil {
		return pick(ctx)
	}
	ctx = context.WithValue(ctx, refreshSelectionContextKey{}, true)
	for {
		waits := m.refreshingCandidates(ctx, providers, model, opts, tried)
		result, err := m.tmxPickWithAdmission(ctx, opts, pick)
		if err == nil || !isAuthUnavailableError(err) {
			return result, err
		}
		if len(waits) == 0 {
			waits = m.refreshingCandidates(ctx, providers, model, opts, tried)
		}
		if len(waits) == 0 {
			return result, err
		}
		waitCtx, done := refreshWaitContext(ctx)
		cases := []reflect.SelectCase{{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(waitCtx.Done())}}
		for _, refreshed := range waits {
			cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(refreshed)})
		}
		chosen, _, _ := reflect.Select(cases)
		done()
		if chosen == 0 {
			if errCtx := ctx.Err(); errCtx != nil {
				return result, errCtx
			}
			return result, err
		}
	}
}

type refreshSelection struct {
	auth     *Auth
	executor ProviderExecutor
	provider string
}

// A channel mutex retains the same serialization as sync.Mutex while allowing
// a request to abandon credential acquisition without waiting for upstream I/O.
type tmxRefreshMutex struct {
	once sync.Once
	gate chan struct{}
}

func (m *tmxRefreshMutex) init() {
	m.once.Do(func() { m.gate = make(chan struct{}, 1); m.gate <- struct{}{} })
}
func (m *tmxRefreshMutex) Lock()   { m.init(); <-m.gate }
func (m *tmxRefreshMutex) Unlock() { m.gate <- struct{}{} }
func (m *tmxRefreshMutex) LockContext(ctx context.Context) error {
	m.init()
	select {
	case <-m.gate:
		// Uncontended: no wait to bound, even once the request's budget is spent.
		return nil
	default:
	}
	bounded, done := refreshWaitContext(ctx)
	defer done()
	select {
	case <-bounded.Done():
		return bounded.Err()
	case <-m.gate:
		if err := bounded.Err(); err != nil {
			m.Unlock()
			return err
		}
		return nil
	}
}
