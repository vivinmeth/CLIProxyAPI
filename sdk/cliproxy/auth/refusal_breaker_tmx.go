package auth

import (
	"strings"
	"time"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/refusals"
	log "github.com/sirupsen/logrus"
)

type tmxMetaRefusalState struct {
	breaker refusals.MetaBreaker
	now     func() time.Time // Explicit clock for deterministic window tests, protected by Manager.mu.
}

func (m *Manager) tmxMetaRefusalSettings() (int, time.Duration) {
	cfg, _ := m.runtimeConfig.Load().(*internalconfig.Config)
	if cfg == nil {
		return (internalconfig.MetaRefusalBreakerConfig{}).Effective()
	}
	return cfg.TMXMetaRefusalBreaker.Effective()
}

func (m *Manager) tmxMetaRefusalNowLocked() time.Time {
	if m.tmxMetaRefusal.now != nil {
		return m.tmxMetaRefusal.now()
	}
	return time.Now()
}

// Called under Manager.mu after stale-result checks, before terminal mutation.
func (m *Manager) tmxLimitMetaRefusalLocked(result Result, a *Auth) Result {
	if result.Success || result.Error == nil || result.Error.Code != refusals.MetaAttestation ||
		statusCodeFromResult(result.Error) != 403 || !strings.EqualFold(strings.TrimSpace(a.Provider), "meta") || tmxAuthDisabled(a) {
		return result
	}
	threshold, window := m.tmxMetaRefusalSettings()
	allow, trip := m.tmxMetaRefusal.breaker.Allow(a.ID, m.tmxMetaRefusalNowLocked(), threshold, window)
	if trip {
		log.Warn(refusals.MetaBreakerAlert)
	}
	if !allow {
		// Replace the terminal evidence before upstream can write a disable or
		// marker. Use the existing ordinary attestation cooldown path instead.
		result.Error = &Error{Code: refusals.AttestationRefused, Message: refusals.AttestationRefused, HTTPStatus: 403}
		result.CredentialScope = false
		result.RetryAfter = nil
	}
	return result
}

// TmxMetaRefusalBreakerStatus exposes only bounded operational state to pool.
// Quiet-window expiry rearms future disables, never any stored account refusal.
func (m *Manager) TmxMetaRefusalBreakerStatus() refusals.MetaBreakerStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	threshold, window := m.tmxMetaRefusalSettings()
	return m.tmxMetaRefusal.breaker.Status(m.tmxMetaRefusalNowLocked(), threshold, window)
}
