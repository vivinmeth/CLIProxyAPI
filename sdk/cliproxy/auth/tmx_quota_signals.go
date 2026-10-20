package auth

// tokenmaxx fork: lets internal/tmx/resets drop an account's passive quota observations after a
// reset credit is redeemed. Without this, reset-soonest keeps ranking the account by the weekly
// reset it saw before the redeem until real traffic brings fresh headers.

import "strings"

// ClearQuotaSignals removes passive quota observations (auth and per-model) for one auth, so
// selectors treat it as unobserved. Cooldown state is untouched; use ResetQuota for that.
// Reports whether anything was cleared.
func (m *Manager) ClearQuotaSignals(authID string) bool {
	authID = strings.TrimSpace(authID)
	if m == nil || authID == "" {
		return false
	}
	releaseMutation := m.lockAuthMutation(authID)
	defer releaseMutation()
	m.mu.Lock()
	auth, ok := m.auths[authID]
	if !ok || auth == nil {
		m.mu.Unlock()
		return false
	}
	changed := auth.Quota.ClearObservationSignals()
	for _, state := range auth.ModelStates {
		if state != nil && state.Quota.ClearObservationSignals() {
			changed = true
		}
	}
	var snapshot *Auth
	if changed {
		auth.Generation++
		snapshot = auth.Clone()
	}
	m.mu.Unlock()
	if snapshot != nil && m.scheduler != nil {
		m.scheduler.upsertAuth(snapshot)
	}
	return changed
}
