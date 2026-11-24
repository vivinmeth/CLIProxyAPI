package auth

import (
	"context"
	"time"
)

// PoolSelectionContext supplies the same configured thresholds, cap and live
// reservation counts as routing, without reserving a slot or binding a session.
func (m *Manager) PoolSelectionContext(ctx context.Context) context.Context {
	if m == nil {
		return ctx
	}
	return context.WithValue(ctx, tmxAdmissionViewKey{}, m.tmxPoolAdmissionView())
}

func (m *Manager) tmxPoolAdmissionView() tmxAdmissionView {
	view := tmxAdmissionView{state: &m.tmxAdmission, five: .95, seven: .98}
	if cfg := m.runtimeConfigSnapshot(); cfg != nil {
		policy := cfg.Routing.ResetSoonest
		if policy.FiveHourThreshold > 0 && policy.FiveHourThreshold <= 1 {
			view.five = policy.FiveHourThreshold
		}
		if policy.SevenDayThreshold > 0 && policy.SevenDayThreshold <= 1 {
			view.seven = policy.SevenDayThreshold
		}
		if policy.MaxInflightPerCredential > 0 {
			view.cap = policy.MaxInflightPerCredential
		}
	}
	return view
}

// TmxOverThresholdWindowsBefore names the windows routing holds at or over threshold for authID,
// for any model and with the configured thresholds, whose reading is older than at. A poll observed
// at at that carried such a window would have refreshed its clock, so these are the windows that
// poll lacked. It reads routing state and never changes it.
func (m *Manager) TmxOverThresholdWindowsBefore(authID string, at time.Time) []string {
	if m == nil {
		return nil
	}
	view := m.tmxPoolAdmissionView()
	m.mu.RLock()
	a := m.auths[authID]
	if a != nil {
		a = a.Clone()
	}
	m.mu.RUnlock()
	if a == nil {
		return nil
	}
	var out []string
	seen := map[string]bool{}
	eachOverQuotaWindow(a, time.Now(), "", true, view, func(window, field string) bool {
		if !seen[window] && a.Quota.SignalObservedTime(field).Before(at) {
			seen[window] = true
			out = append(out, window)
		}
		return true
	})
	return out
}
