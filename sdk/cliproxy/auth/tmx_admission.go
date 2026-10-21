package auth

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	executor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

type tmxAdmission struct {
	mu     sync.Mutex
	counts sync.Map
}

func (a *tmxAdmission) counter(id string) *atomic.Int64 {
	v, _ := a.counts.LoadOrStore(id, &atomic.Int64{})
	return v.(*atomic.Int64)
}

// InFlightCount reports upstream requests currently holding an admission
// reservation on the credential (reset-soonest only; 0 otherwise).
func (m *Manager) InFlightCount(authID string) int {
	if m == nil {
		return 0
	}
	v, ok := m.tmxAdmission.counts.Load(authID)
	if !ok {
		return 0
	}
	return int(v.(*atomic.Int64).Load())
}

type tmxAdmissionKey struct{}
type tmxAdmissionViewKey struct{}
type tmxAdmissionView struct {
	state       *tmxAdmission
	five, seven float64
	cap         int
}

func (v tmxAdmissionView) count(id string) int64 {
	if v.state == nil {
		return 0
	}
	return v.state.counter(id).Load()
}

type tmxReservation struct {
	mu          sync.Mutex
	release     func()
	closed      bool
	streamOwned atomic.Bool
	stop        func() bool
}

func (r *tmxReservation) clear() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.release != nil {
		r.release()
		r.release = nil
	}
}
func (r *tmxReservation) close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	if r.release != nil {
		r.release()
		r.release = nil
	}
}
func (r *tmxReservation) reserve(counter *atomic.Int64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return false
	}
	counter.Add(1)
	var once sync.Once
	r.release = func() { once.Do(func() { counter.Add(-1) }) }
	return true
}

func (m *Manager) tmxPickWithAdmission(ctx context.Context, opts executor.Options, pick func(context.Context) (refreshSelection, error)) (refreshSelection, error) {
	reservation, _ := ctx.Value(tmxAdmissionKey{}).(*tmxReservation)
	m.mu.RLock()
	selector := m.selector
	m.mu.RUnlock()
	outer := selector
	if affinity, ok := selector.(*SessionAffinitySelector); ok {
		selector = affinity.fallback
	}
	if _, ok := selector.(*ResetSoonestSelector); !ok {
		return pick(ctx)
	}
	view := m.tmxPoolAdmissionView()
	if reservation == nil {
		return pick(context.WithValue(ctx, tmxAdmissionViewKey{}, view))
	}
	// Body parsing for session affinity runs before the mutex; it holds only rank + reserve.
	ctx = prepareTMXSession(ctx, outer, opts)
	m.tmxAdmission.mu.Lock()
	defer m.tmxAdmission.mu.Unlock()
	reservation.clear()
	if err := ctx.Err(); err != nil {
		return refreshSelection{}, err
	}
	result, err := pick(context.WithValue(context.WithValue(ctx, tmxAdmissionViewKey{}, view), tmxUnderAdmissionKey{}, true))
	if err == nil && result.auth != nil && !reservation.reserve(m.tmxAdmission.counter(result.auth.ID)) {
		return refreshSelection{}, context.Canceled
	}
	return result, err
}

func newTMXReservation(ctx context.Context) (context.Context, *tmxReservation, func() bool) {
	if ctx == nil {
		ctx = context.Background()
	}
	reservation := &tmxReservation{}
	stop := context.AfterFunc(ctx, reservation.close)
	reservation.stop = stop
	return context.WithValue(ctx, tmxAdmissionKey{}, reservation), reservation, stop
}

// The reservation covers selection, preparation, refresh, and the full upstream
// response lifetime. Defers release it even if an executor panics.
func (m *Manager) Execute(ctx context.Context, providers []string, req executor.Request, opts executor.Options) (executor.Response, error) {
	ctx = withTMXRequestBudgets(ctx)
	if !m.tmxAdmissionEnabled() {
		return m.executeWithoutAdmission(ctx, providers, req, opts)
	}
	ctx, r, stop := newTMXReservation(ctx)
	defer stop()
	defer r.close()
	return m.executeWithoutAdmission(ctx, providers, req, opts)
}
func (m *Manager) ExecuteCount(ctx context.Context, providers []string, req executor.Request, opts executor.Options) (executor.Response, error) {
	ctx = withTMXRequestBudgets(ctx)
	if !m.tmxAdmissionEnabled() {
		return m.executeCountWithoutAdmission(ctx, providers, req, opts)
	}
	ctx, r, stop := newTMXReservation(ctx)
	defer stop()
	defer r.close()
	return m.executeCountWithoutAdmission(ctx, providers, req, opts)
}
func (m *Manager) ExecuteStream(ctx context.Context, providers []string, req executor.Request, opts executor.Options) (result *executor.StreamResult, err error) {
	ctx = withTMXRequestBudgets(ctx)
	if !m.tmxAdmissionEnabled() {
		return m.executeStreamWithoutAdmission(ctx, providers, req, opts)
	}
	ctx, r, stop := newTMXReservation(ctx)
	defer func() {
		if err != nil || result == nil || !r.streamOwned.Load() {
			stop()
			r.close()
		}
	}()
	result, err = m.executeStreamWithoutAdmission(ctx, providers, req, opts)
	return result, err
}

// Called by the existing stream forwarder before it starts, preserving its
// completion/bookkeeping order and avoiding another forwarding goroutine.
func tmxStreamReservationCompletion(ctx context.Context) func() {
	if ctx == nil {
		return func() {}
	}
	r, _ := ctx.Value(tmxAdmissionKey{}).(*tmxReservation)
	if r == nil {
		return func() {}
	}
	r.streamOwned.Store(true)
	return func() { r.stop(); r.close() }
}

func admissionView(ctx context.Context) tmxAdmissionView {
	if ctx != nil {
		if v, ok := ctx.Value(tmxAdmissionViewKey{}).(tmxAdmissionView); ok {
			return v
		}
	}
	return tmxAdmissionView{five: .95, seven: .98}
}

func (s *SessionAffinitySelector) tmxBoundPick(ctx context.Context, provider, model string, opts executor.Options, bound *Auth, available []*Auth) (*Auth, error) {
	if _, ok := s.fallback.(*ResetSoonestSelector); !ok {
		return bound, nil
	}
	view := admissionView(ctx)
	// Binding remains authoritative. Saturation permits only a transient spill;
	// callers retain/refresh the original binding before returning this choice.
	if view.cap <= 0 || view.count(bound.ID) < int64(view.cap) {
		return bound, nil
	}
	alternatives := make([]*Auth, 0, len(available))
	for _, a := range available {
		if a.ID != bound.ID {
			alternatives = append(alternatives, a)
		}
	}
	if len(alternatives) == 0 {
		return bound, nil
	}
	pick := pickResetSoonestForModel(highestPriorityAuths(alternatives), time.Now(), model, view)
	if pick != nil && view.count(pick.ID) < view.count(bound.ID) {
		if opts.Metadata != nil {
			opts.Metadata[tmxAdmissionSpillKey] = bound.ID
		}
		return pick, nil
	}
	return bound, nil
}

const tmxAdmissionSpillKey = "tmx_admission_bound_auth"

func tmxPreserveAdmissionSpill(res Result) bool {
	bound, _ := res.Options.Metadata[tmxAdmissionSpillKey].(string)
	return bound != "" && res.AuthID != bound
}

func (m *Manager) tmxAdmissionEnabled() bool {
	m.mu.RLock()
	selector := m.selector
	m.mu.RUnlock()
	if affinity, ok := selector.(*SessionAffinitySelector); ok {
		selector = affinity.fallback
	}
	_, ok := selector.(*ResetSoonestSelector)
	return ok && !m.HomeEnabled()
}
