package auth

// tokenmaxx fork: lets internal/tmx/resets drop an account's passive quota observations after a
// reset credit is redeemed. Without this, reset-soonest keeps ranking the account by the weekly
// reset it saw before the redeem until real traffic brings fresh headers.

import (
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
)

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

// QuotaWindowObservation is an external (usage API or restored history) window.
// UsedPercent uses the API's 0..100 scale, unlike Claude's passive fraction.
type QuotaWindowObservation struct {
	Window      string
	UsedPercent *float64
	ResetsAt    time.Time
	Status      string
	ObservedAt  time.Time
}

// ApplyQuotaObservation merges fresh external readings into routing observations.
// It deliberately does not create cooldowns or persist cooldown state.
func (m *Manager) ApplyQuotaObservation(authID, provider string, windows []QuotaWindowObservation) bool {
	if m == nil {
		return false
	}
	now := time.Now()
	release := m.lockAuthMutation(authID)
	defer release()
	m.mu.Lock()
	a := m.auths[authID]
	if a == nil || !strings.EqualFold(a.Provider, provider) {
		m.mu.Unlock()
		return false
	}
	changed := false
	for _, w := range windows {
		if w.ObservedAt.IsZero() || now.Sub(w.ObservedAt) > QuotaObservationMaxAge || w.ObservedAt.After(now.Add(time.Minute)) || !w.ResetsAt.After(now) {
			continue
		}
		prefix := ""
		switch strings.ToLower(provider) {
		case "claude":
			window := w.Window
			if strings.HasPrefix(window, "7d:fable") {
				window = "7d_oi"
			} else if strings.HasPrefix(window, "7d:") {
				slug := strings.TrimPrefix(window, "7d:")
				if !validQuotaModelSlug(slug) {
					continue
				}
				window = "7d_model_" + slug
			}
			if window != "5h" && window != "7d" && !strings.HasPrefix(window, "7d_") {
				continue
			}
			prefix = "Anthropic-Ratelimit-Unified-" + window + "-"
		case "codex":
			switch w.Window {
			case "5h":
				prefix = "X-Codex-Primary-"
			case "7d":
				prefix = "X-Codex-Secondary-"
			default:
				continue
			}
		default:
			continue
		}
		fields := map[string]string{}
		resetKey := prefix + "Reset"
		utilKey := prefix + "Utilization"
		if strings.EqualFold(provider, "codex") {
			resetKey = prefix + "Reset-At"
			utilKey = prefix + "Used-Percent"
			minutes := "300"
			if w.Window == "7d" {
				minutes = "10080"
			}
			fields[prefix+"Window-Minutes"] = minutes
		}
		fields[resetKey] = strconv.FormatInt(w.ResetsAt.Unix(), 10)
		if w.UsedPercent != nil && !math.IsNaN(*w.UsedPercent) && !math.IsInf(*w.UsedPercent, 0) && *w.UsedPercent >= 0 && *w.UsedPercent <= 100 {
			used := *w.UsedPercent
			if strings.EqualFold(provider, "claude") {
				used /= 100
			}
			fields[utilKey] = strconv.FormatFloat(used, 'f', -1, 64)
		}
		if validQuotaSignalValue(w.Status) {
			fields[prefix+"Status"] = w.Status
		}
		// Each field is compared independently so a newer 5h reading doesn't block
		// filling an older, still-current 7d window.
		for key := range fields {
			if _, exists := a.Quota.Signals[http.CanonicalHeaderKey(key)]; exists && !w.ObservedAt.After(a.Quota.SignalObservedTime(key)) {
				delete(fields, key)
			} else if a.Quota.signalClearedSince(key, w.ObservedAt) {
				// Read before a reset credit cleared this window.
				delete(fields, key)
			}
		}
		if len(fields) == 0 {
			continue
		}
		a.Quota.mergeObservedSignals(fields, w.ObservedAt)
		changed = true
	}
	var snapshot *Auth
	if changed {
		a.Generation++
		snapshot = a.Clone()
	}
	m.mu.Unlock()
	if snapshot != nil && m.scheduler != nil {
		m.scheduler.upsertAuth(snapshot)
	}
	return changed
}

func validQuotaModelSlug(slug string) bool {
	if slug == "" || len(slug) > 80 {
		return false
	}
	for _, c := range slug {
		if !(c >= 'a' && c <= 'z') && !(c >= '0' && c <= '9') && c != '-' && c != '_' {
			return false
		}
	}
	return true
}
