package auth

// tokenmaxx fork: the first 429 a client receives carries Retry-After. Before this only a
// cached-cooldown refusal (modelCooldownError, Home errors) had one; a refusal straight from the
// provider reached the client without a hint even though the executor had parsed its deadline.

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

// statusOverloaded is Anthropic's non-standard "overloaded" status.
const statusOverloaded = 529

// maxClientRetryAfter caps the hint given to clients. A weekly quota reset days away is still
// reported, as "retry in a day", so a client never sleeps for most of a week on one answer.
const maxClientRetryAfter = 24 * time.Hour

// upstreamRetryAfterHeader returns Retry-After for an upstream refusal whose status allows the
// header (429, 503, 529) and whose executor parsed a deadline. Unknown deadlines get no header.
func upstreamRetryAfterHeader(err error) http.Header {
	switch statusCodeFromError(err) {
	case http.StatusTooManyRequests, http.StatusServiceUnavailable, statusOverloaded:
	default:
		return nil
	}
	retryAfter := retryAfterFromError(err)
	if retryAfter == nil || *retryAfter <= 0 {
		return nil
	}
	return safeRetryAfterHeader(min(*retryAfter, maxClientRetryAfter))
}

// poolRetryAfterError reports the pool's recovery as the client's Retry-After for a refusal that
// exhausted the pool. The upstream error (status, body, headers) is otherwise unchanged, and the
// per-credential deadlines MarkResult recorded are untouched.
type poolRetryAfterError struct {
	error
	retryAfter time.Duration
}

func (e *poolRetryAfterError) Unwrap() error { return e.error }

func (e *poolRetryAfterError) RetryAfter() *time.Duration {
	retryAfter := e.retryAfter
	return &retryAfter
}

func (e *poolRetryAfterError) StatusCode() int { return statusCodeFromError(e.error) }

func (e *poolRetryAfterError) Headers() http.Header {
	var headered interface{ Headers() http.Header }
	if errors.As(e.error, &headered) && headered != nil {
		return headered.Headers()
	}
	return nil
}

// withPoolRetryAfter gives a pool-exhausting upstream refusal (429, 503, 529) the soonest
// recovery among the route's eligible credentials, so the answer doesn't depend on which
// credential was tried last. It is left alone while any eligible credential is usable now or a
// blocked one has no known recovery.
func (m *Manager) withPoolRetryAfter(ctx context.Context, err error, providers []string, model string, opts cliproxyexecutor.Options) error {
	if m == nil || err == nil || m.HomeEnabled() {
		return err
	}
	switch statusCodeFromError(err) {
	case http.StatusTooManyRequests, http.StatusServiceUnavailable, statusOverloaded:
	default:
		return err
	}
	providerSet := make(map[string]struct{}, len(providers))
	for _, provider := range providers {
		if p := canonicalSchedulingProvider(provider); p != "" {
			providerSet[p] = struct{}{}
		}
	}
	eligibility := authSelectionEligibilityForRequest(ctx, opts)
	pinned := pinnedAuthIDFromMetadata(opts.Metadata)
	registryRef := registry.GetGlobalRegistry()
	now := time.Now()
	var soonest time.Time
	m.mu.RLock()
	defer m.mu.RUnlock()
	for id, a := range m.auths {
		if a == nil || a.Disabled || !eligibility.allows(a) || (pinned != "" && pinned != id) {
			continue
		}
		if _, ok := providerSet[canonicalSchedulingProvider(executorKeyFromAuth(a))]; !ok {
			continue
		}
		if model != "" && !m.authSupportsRouteModel(registryRef, a, model) {
			continue
		}
		blocked, _, next := isAuthBlockedForModel(a, m.selectionModelKeyForAuth(a, model), now)
		if !blocked || !next.After(now) {
			return err
		}
		if soonest.IsZero() || next.Before(soonest) {
			soonest = next
		}
	}
	if soonest.IsZero() {
		return err
	}
	return &poolRetryAfterError{error: err, retryAfter: soonest.Sub(now)}
}
