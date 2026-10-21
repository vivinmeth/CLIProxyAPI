package auth

// tokenmaxx fork: after a reset credit is redeemed, internal/tmx/resets releases only what the
// credit verifiably restored. ResetQuota would reopen every model and drop every block (auth
// failures, transient errors, independent model limits) before anyone checked that the weekly
// window actually recovered.

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
)

// QuotaRecovery is what a redeemed reset credit restored on one account.
type QuotaRecovery struct {
	// Windows recovered ("5h", "7d", "7d_oi", "30d", or a Codex slot name such as "primary").
	// Their passive quota signals are dropped so selectors stop ranking the account by them.
	Windows []string
	// CredentialQuota releases the credential-wide quota cooldown (a shared window rejected the
	// account and none is exhausted any more), together with the rejection markers in the
	// signals (Retry-After, unified status/reset, limit-reached).
	CredentialQuota bool
	// ModelWindows are recovered model windows (Claude's 7d_oi for Fable, 7d_opus, 7d_sonnet,
	// 7d_model_<slug>). They release the model-scoped quota cooldowns of matching models only,
	// including one held beneath a credential quota; other models' limits stay.
	ModelWindows []string
}

// ClearRecoveredQuota applies r to one auth. Unauthorized, transient, model-support and other
// non-quota blocks are never touched, and an independent model block beneath a released
// credential quota is reinstated. Reports whether anything changed.
func (m *Manager) ClearRecoveredQuota(ctx context.Context, authID string, r QuotaRecovery) (bool, error) {
	authID = strings.TrimSpace(authID)
	if m == nil || authID == "" {
		return false, nil
	}
	now := time.Now()
	releaseMutation := m.lockAuthMutation(authID)
	defer releaseMutation()
	m.mu.Lock()
	auth, ok := m.auths[authID]
	if !ok || auth == nil {
		m.mu.Unlock()
		return false, nil
	}
	trackCooldownState := m.cooldownStore != nil
	var cooldownRecordsBefore []CooldownStateRecord
	if trackCooldownState {
		cooldownRecordsBefore = m.cooldownStateRecordsForAuthLocked(auth, now)
	}

	changed := dropRecoveredQuotaSignals(&auth.Quota, auth.Provider, r)
	for _, state := range auth.ModelStates {
		if state != nil && dropRecoveredQuotaSignals(&state.Quota, auth.Provider, r) {
			changed = true
		}
	}
	if !hasUnauthorizedAuthFailure(auth) && releaseRecoveredQuotaCooldowns(auth, r, now) {
		changed = true
	}
	if !changed {
		m.mu.Unlock()
		return false, nil
	}
	auth.Generation++
	auth.UpdatedAt = now
	snapshot := auth.Clone()
	cooldownStateChanged := false
	if trackCooldownState {
		cooldownStateChanged = !cooldownStateRecordsEqual(cooldownRecordsBefore, m.cooldownStateRecordsForAuthLocked(auth, now))
	}
	errPersist := m.persistLocked(ctx, auth)
	m.mu.Unlock()
	releaseMutation()
	if cooldownStateChanged {
		m.persistCooldownStates(context.Background())
	}

	supportedModels, regEpoch := registry.GetGlobalRegistry().GetModelsAndEpochForClient(authID)
	projections := make([]registry.ClientModelProjection, 0, len(supportedModels))
	for _, sm := range supportedModels {
		if sm == nil || strings.TrimSpace(sm.ID) == "" {
			continue
		}
		projections = append(projections, m.clientModelProjectionForAuth(snapshot, sm.ID, now))
	}
	registry.GetGlobalRegistry().ApplyClientModelProjections(authID, regEpoch, snapshot.Generation, projections)
	if m.scheduler != nil {
		m.scheduler.upsertAuth(snapshot)
	}
	return true, errPersist
}

// releaseRecoveredQuotaCooldowns lifts only quota layers, mirroring the Devin quota restoration.
func releaseRecoveredQuotaCooldowns(auth *Auth, r QuotaRecovery, now time.Time) bool {
	changed := false
	if r.CredentialQuota && auth.Quota.Exceeded && (auth.Quota.Reason == "credential_quota" || auth.Quota.Reason == "quota") {
		// An auth-level deadline equal to the quota deadline is that quota's; any other
		// (a later transient or auth block) stays.
		if !auth.NextRetryAfter.After(auth.Quota.NextRecoverAt) {
			auth.NextRetryAfter = time.Time{}
			auth.Unavailable = false
		}
		applyCooldownFields(&auth.Quota, QuotaState{})
		changed = true
	}
	for model, state := range auth.ModelStates {
		if state == nil || !state.Quota.Exceeded {
			continue
		}
		reason := strings.TrimSpace(state.Quota.Reason)
		wrapped := state.credentialQuotaPropagated || reason == "credential_quota"
		shadow := state.credentialQuotaShadow
		switch {
		case wrapped && r.CredentialQuota:
			// Reinstate the independent layer beneath the propagated credential quota, unless
			// that layer is a model quota this credit recovered too.
			if shadow != nil && isModelStateActiveCooldown(shadow, now) && !r.releasesModelQuota(model, shadow) {
				*state = *shadow.cloneShallow()
				state.UpdatedAt = now
			} else {
				resetModelState(state, now)
			}
			changed = true
		case wrapped && r.releasesModelQuota(model, shadow):
			// The shared block stays; the recovered model quota must not resurface when it ends.
			state.credentialQuotaShadow = nil
			changed = true
		case !wrapped && r.releasesModelQuota(model, state):
			resetModelState(state, now)
			changed = true
		}
	}
	if !changed {
		return false
	}
	if len(auth.ModelStates) > 0 {
		updateAggregatedAvailability(auth, now)
	}
	if !auth.Unavailable && !hasModelError(auth, now) && quotaStatusOnly(auth) {
		auth.Status = StatusActive
		auth.StatusMessage = ""
		auth.LastError = nil
	}
	return true
}

// releasesModelQuota reports whether state is a model-scoped quota layer of model that one of
// r's recovered model windows limits.
func (r QuotaRecovery) releasesModelQuota(model string, state *ModelState) bool {
	if state == nil || !state.Quota.Exceeded || strings.TrimSpace(state.Quota.Reason) != "quota" {
		return false
	}
	for _, window := range r.ModelWindows {
		if modelWindowLimits(window, model) {
			return true
		}
	}
	return false
}

// modelWindowLimits reports whether a Claude model window limits model, matching the families
// routing ranks by (claudeWeeklyResetNames).
func modelWindowLimits(window, model string) bool {
	window = strings.ToLower(strings.TrimSpace(window))
	model = strings.ToLower(model)
	var family string
	switch {
	case window == "7d_oi":
		family = "fable"
	case strings.HasPrefix(window, "7d_model_"):
		family = strings.TrimPrefix(window, "7d_model_")
	case window == "7d_opus" || window == "7d_sonnet":
		family = strings.TrimPrefix(window, "7d_")
	}
	return family != "" && strings.Contains(model, family)
}

// quotaStatusOnly reports whether the auth's status describes a quota refusal and nothing else.
func quotaStatusOnly(auth *Auth) bool {
	if auth.LastError != nil {
		return statusCodeFromResult(auth.LastError) == 429
	}
	return auth.StatusMessage == "" || auth.StatusMessage == "quota exhausted"
}

// claudeRejectionSignals and codexRejectionSignals describe a refusal as a whole rather than one
// window; they go once the credential quota is released.
var (
	claudeRejectionSignals = []string{"retry-after", "anthropic-ratelimit-unified-status",
		"anthropic-ratelimit-unified-reset", "anthropic-ratelimit-unified-representative-claim"}
	codexRejectionSignals = []string{"retry-after", "x-codex-limit-reached", "x-codex-allowed"}
)

func dropRecoveredQuotaSignals(q *QuotaState, provider string, r QuotaRecovery) bool {
	if q == nil || len(q.Signals) == 0 {
		return false
	}
	recovered := make(map[string]bool, len(r.Windows))
	for _, w := range r.Windows {
		recovered[strings.ToLower(strings.TrimSpace(w))] = true
	}
	var prefixes, exact []string
	switch strings.ToLower(strings.TrimSpace(provider)) {
	case "claude":
		for _, w := range []string{"5h", "7d", "7d_oi"} {
			if recovered[w] {
				prefixes = append(prefixes, "anthropic-ratelimit-unified-"+w+"-")
			}
		}
		if r.CredentialQuota {
			exact = claudeRejectionSignals
		}
	case "codex":
		for _, slot := range []string{"primary", "secondary"} {
			if recovered[codexSignalWindowName(q.Signals, slot)] {
				prefixes = append(prefixes, "x-codex-"+slot+"-")
			}
		}
		if r.CredentialQuota {
			exact = codexRejectionSignals
		}
	}
	changed := false
	for key := range q.Signals {
		lower := strings.ToLower(key)
		drop := false
		for _, name := range exact {
			drop = drop || lower == name
		}
		for _, prefix := range prefixes {
			drop = drop || strings.HasPrefix(lower, prefix)
		}
		if drop {
			delete(q.Signals, key)
			changed = true
		}
	}
	if changed && len(q.Signals) == 0 {
		q.Signals = nil
		q.ObservedAt = time.Time{}
	}
	return changed
}

// codexSignalWindowName names a Codex slot the way internal/tmx/resets names its windows: by its
// length (5h, 7d, 30d), falling back to the slot name.
func codexSignalWindowName(signals map[string]string, slot string) string {
	var raw string
	for key, value := range signals {
		if strings.EqualFold(key, "x-codex-"+slot+"-window-minutes") {
			raw = value
		}
	}
	minutes, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	switch {
	case err != nil:
		return slot
	case minutes >= 28*24*60:
		return "30d"
	case minutes >= 7*24*60:
		return "7d"
	case minutes > 0 && minutes <= 5*60:
		return "5h"
	}
	return slot
}
