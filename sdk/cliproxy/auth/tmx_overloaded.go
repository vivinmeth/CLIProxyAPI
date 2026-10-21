package auth

// tokenmaxx fork: a 529 (Anthropic "overloaded") is a provider-wide, seconds-long condition, not
// a problem with the credential. It gets the same retry-round treatment as a 503 and a short
// cooldown instead of the generic 60 s one, so the pool is not drained by a brief overload.

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"
)

// overloadedCooldown is the cooldown after a 529 without a Retry-After hint.
const overloadedCooldown = 10 * time.Second

// overloadedFailureRetryAfter is recoverableFailureRetryAfterWithHint for a 529: the provider's
// hint wins, otherwise the shorter of overloadedCooldown and a configured transient cooldown.
func overloadedFailureRetryAfter(now time.Time, retryAfter *time.Duration, disableCooling bool) time.Time {
	if disableCooling {
		return time.Time{}
	}
	seconds := transientErrorCooldownSeconds.Load()
	if seconds < 0 {
		return time.Time{}
	}
	if retryAfter != nil && *retryAfter > 0 {
		return now.Add(*retryAfter)
	}
	cooldown := overloadedCooldown
	if configured := time.Duration(seconds) * time.Second; seconds > 0 && configured < cooldown {
		cooldown = configured
	}
	return now.Add(cooldown)
}

// A 529 says the provider (for a model) is overloaded, so trying every credential of the pool
// again on every retry round only adds load. Overload therefore gets its own pacing; quota and
// other failover are unchanged:
//   - a provider/model backoff, noted on every 529 (its Retry-After, else the short overload
//     cooldown), that spaces retry rounds for every request on that route;
//   - per client request, at most overloadCredentialsPerRound credentials answering 529 per round
//     (one while a backoff is already active) and overloadRequestBudget in total.
const (
	overloadCredentialsPerRound = 2
	overloadRequestBudget       = 4
)

type tmxOverload struct {
	mu    sync.Mutex
	until map[string]time.Time
}

func overloadKey(provider, model string) string {
	return canonicalSchedulingProvider(provider) + "\x00" + canonicalModelKey(model)
}

// noteOverload records a 529 for the route and charges the request's overload budget.
func (m *Manager) noteOverload(ctx context.Context, result Result) {
	if m == nil || statusCodeFromResult(result.Error) != statusOverloaded {
		return
	}
	if budget, _ := ctx.Value(overloadBudgetKey{}).(*overloadBudget); budget != nil {
		budget.note()
	}
	now := time.Now()
	until := overloadedFailureRetryAfter(now, result.RetryAfter, false)
	if until.IsZero() {
		return
	}
	model := result.RouteModel
	if strings.TrimSpace(model) == "" {
		model = result.Model
	}
	m.tmxOverload.mu.Lock()
	defer m.tmxOverload.mu.Unlock()
	if m.tmxOverload.until == nil {
		m.tmxOverload.until = make(map[string]time.Time)
	}
	for key, at := range m.tmxOverload.until {
		if !at.After(now) {
			delete(m.tmxOverload.until, key)
		}
	}
	key := overloadKey(result.Provider, model)
	if until.After(m.tmxOverload.until[key]) {
		m.tmxOverload.until[key] = until
	}
}

// overloadBackoff is how long the route's overload backoff still runs.
func (m *Manager) overloadBackoff(providers []string, model string) time.Duration {
	if m == nil {
		return 0
	}
	now := time.Now()
	var longest time.Duration
	m.tmxOverload.mu.Lock()
	defer m.tmxOverload.mu.Unlock()
	for _, provider := range providers {
		if remaining := m.tmxOverload.until[overloadKey(provider, model)].Sub(now); remaining > longest {
			longest = remaining
		}
	}
	return longest
}

type overloadBudgetKey struct{}

type overloadBudget struct {
	mu       sync.Mutex
	total    int
	round    int
	inRound  int
	roundCap int
	waited   time.Duration
}

func (b *overloadBudget) note() {
	b.mu.Lock()
	b.total++
	b.inRound++
	b.mu.Unlock()
}

func withOverloadBudget(ctx context.Context) context.Context {
	if _, ok := ctx.Value(overloadBudgetKey{}).(*overloadBudget); ok {
		return ctx
	}
	return context.WithValue(ctx, overloadBudgetKey{}, &overloadBudget{round: -1})
}

// withTMXRequestBudgets gives a client request its refresh-wait and overload budgets.
func withTMXRequestBudgets(ctx context.Context) context.Context {
	return withOverloadBudget(withRefreshWaitBudget(ctx))
}

// overloadRoundSpent reports, before the next credential of retryRound is tried, whether the
// request's overload budget for the round or in total is used up.
func (m *Manager) overloadRoundSpent(ctx context.Context, retryRound int, providers []string, model string) bool {
	budget, _ := ctx.Value(overloadBudgetKey{}).(*overloadBudget)
	if budget == nil {
		return false
	}
	backoff := m.overloadBackoff(providers, model) > 0
	budget.mu.Lock()
	defer budget.mu.Unlock()
	if budget.round != retryRound {
		budget.round, budget.inRound, budget.roundCap = retryRound, 0, overloadCredentialsPerRound
		if backoff {
			budget.roundCap = 1
		}
	}
	return budget.total >= overloadRequestBudget || budget.inRound >= budget.roundCap
}

// defaultOverloadMaxWait is routing.overload-max-wait when unset or invalid.
const defaultOverloadMaxWait = 5 * time.Second

// overloadClientRetryAfterCap caps the Retry-After of a 529 returned once pacing gives up.
const overloadClientRetryAfterCap = time.Minute

// overloadMaxWait is the configured total overload wait per client request.
func (m *Manager) overloadMaxWait() time.Duration {
	if cfg := m.runtimeConfigSnapshot(); cfg != nil {
		if raw := strings.TrimSpace(cfg.Routing.OverloadMaxWait); raw != "" {
			if d, err := time.ParseDuration(raw); err == nil && d >= 0 {
				return d
			}
		}
	}
	return defaultOverloadMaxWait
}

// overloadRetryWait spaces the next retry round after a 529 by the route's overload backoff, and
// ends the retries once the request's overload budget is spent, the backoff exceeds maxWait, or
// the wait no longer fits the request's overload-max-wait. It returns the wait (jitter included,
// already charged to the request) and the ceiling to pass to waitForCooldown with it.
func (m *Manager) overloadRetryWait(ctx context.Context, err error, providers []string, model string, wait, maxWait time.Duration) (time.Duration, time.Duration, bool) {
	if !isOverloadError(err) {
		return wait, maxWait, true
	}
	budget, _ := ctx.Value(overloadBudgetKey{}).(*overloadBudget)
	if budget != nil {
		budget.mu.Lock()
		spent := budget.total >= overloadRequestBudget
		budget.mu.Unlock()
		if spent {
			return 0, 0, false
		}
	}
	if backoff := m.overloadBackoff(providers, model); backoff > wait {
		if maxWait <= 0 || backoff > maxWait {
			return 0, 0, false
		}
		wait = backoff
	}
	if budget == nil || wait <= 0 {
		return wait, maxWait, true
	}
	budget.mu.Lock()
	defer budget.mu.Unlock()
	remaining := m.overloadMaxWait() - budget.waited
	if wait > remaining {
		return 0, 0, false
	}
	ceiling := remaining
	if maxWait > 0 && maxWait < ceiling {
		ceiling = maxWait
	}
	wait = jitteredCooldownWait(wait, ceiling)
	budget.waited += wait
	return wait, wait, true
}

// isOverloadError reports whether err is a 529, or a local refusal (every credential cooling
// down, no auth available) whose last upstream cause was one: concurrent requests that find the
// overloaded credential still cooling wait on the same overload, and charge the same budget.
func isOverloadError(err error) bool {
	for depth := 0; err != nil && depth < 8; depth++ {
		if coder, ok := err.(interface{ StatusCode() int }); ok && coder.StatusCode() == statusOverloaded {
			return true
		}
		err = errors.Unwrap(err)
	}
	return false
}

// overloadRetryAfter gives a 529 that ends a request the route's remaining overload backoff as
// Retry-After (capped), so the client paces itself like the pool does.
func (m *Manager) overloadRetryAfter(err error, providers []string, model string) (error, bool) {
	if statusCodeFromError(err) != statusOverloaded {
		return err, false
	}
	backoff := m.overloadBackoff(providers, model)
	if backoff <= 0 {
		return err, false
	}
	return &poolRetryAfterError{error: err, retryAfter: min(backoff, overloadClientRetryAfterCap)}, true
}
