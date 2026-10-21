package auth

import "context"

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
