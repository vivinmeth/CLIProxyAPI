package auth

import (
	"context"
	"reflect"
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

func (m *Manager) refreshingCandidates(ctx context.Context, providers []string, opts cliproxyexecutor.Options, tried map[string]struct{}) []<-chan struct{} {
	eligibility := authSelectionEligibilityForRequest(ctx, opts)
	pinned := pinnedAuthIDFromMetadata(opts.Metadata)
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
		if !matches {
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

func retryDuringRefresh[T any](m *Manager, ctx context.Context, providers []string, opts cliproxyexecutor.Options, tried map[string]struct{}, pick func(context.Context) (T, error)) (T, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if ctx.Value(refreshSelectionContextKey{}) != nil {
		return pick(ctx)
	}
	ctx = context.WithValue(ctx, refreshSelectionContextKey{}, true)
	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for {
		waits := m.refreshingCandidates(ctx, providers, opts, tried)
		result, err := pick(ctx)
		if err == nil || !isAuthUnavailableError(err) {
			return result, err
		}
		if len(waits) == 0 {
			waits = m.refreshingCandidates(ctx, providers, opts, tried)
		}
		if len(waits) == 0 {
			return result, err
		}
		cases := []reflect.SelectCase{{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(waitCtx.Done())}}
		for _, done := range waits {
			cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(done)})
		}
		if chosen, _, _ := reflect.Select(cases); chosen == 0 {
			return result, waitCtx.Err()
		}
	}
}

type refreshSelection struct {
	auth     *Auth
	executor ProviderExecutor
	provider string
}
