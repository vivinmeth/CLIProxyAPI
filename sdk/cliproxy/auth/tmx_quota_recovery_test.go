package auth

import (
	"context"
	"net/http"
	"testing"
	"time"
)

func registerRecoveryAuth(t *testing.T, id, provider string, signals map[string]string) *Manager {
	t.Helper()
	withQuotaCooldownEnabled(t)
	manager := NewManager(nil, nil, nil)
	auth := &Auth{ID: id, Provider: provider}
	if signals != nil {
		auth.Quota.Signals = signals
		auth.Quota.ObservedAt = time.Now()
	}
	if _, err := manager.Register(WithSkipPersist(context.Background()), auth); err != nil {
		t.Fatalf("register auth: %v", err)
	}
	return manager
}

func markRecoveryResult(manager *Manager, authID, model string, status int, retryAfter time.Duration, credentialScope bool) {
	result := Result{AuthID: authID, Provider: "claude", Model: model, CredentialScope: credentialScope,
		Error: &Error{HTTPStatus: status, Message: http.StatusText(status)}}
	if retryAfter > 0 {
		result.RetryAfter = &retryAfter
	}
	manager.MarkResult(context.Background(), result)
}

func recoveryState(t *testing.T, manager *Manager, authID string) *Auth {
	t.Helper()
	auth, ok := manager.GetByID(authID)
	if !ok || auth == nil {
		t.Fatal("auth missing")
	}
	return auth
}

func TestClearRecoveredQuotaReleasesOnlyQuotaLayers(t *testing.T) {
	const id = "recovery-credential"
	manager := registerRecoveryAuth(t, id, "claude", nil)
	now := time.Now()
	// An independent transient block, a model-scoped Fable limit, then a credential-wide weekly limit.
	markRecoveryResult(manager, id, "claude-sonnet-5", http.StatusServiceUnavailable, 10*time.Minute, false)
	markRecoveryResult(manager, id, "claude-fable-5", http.StatusTooManyRequests, 72*time.Hour, false)
	markRecoveryResult(manager, id, "claude-opus-5", http.StatusTooManyRequests, 72*time.Hour, true)
	if auth := recoveryState(t, manager, id); auth.Quota.Reason != "credential_quota" {
		t.Fatalf("setup: credential quota not applied: %+v", auth.Quota)
	}

	changed, err := manager.ClearRecoveredQuota(context.Background(), id, QuotaRecovery{CredentialQuota: true})
	if err != nil || !changed {
		t.Fatalf("ClearRecoveredQuota = %v, %v", changed, err)
	}
	auth := recoveryState(t, manager, id)
	if auth.Quota.Reason == "credential_quota" {
		t.Fatalf("credential quota still applied: %+v", auth.Quota)
	}
	if opus := auth.ModelStates["claude-opus-5"]; opus.Unavailable || opus.Quota.Exceeded {
		t.Fatalf("the reporting model must be released: %+v", opus)
	}
	sonnet := auth.ModelStates["claude-sonnet-5"]
	if !sonnet.Unavailable || sonnet.NextRetryAfter.Before(now.Add(9*time.Minute)) {
		t.Fatalf("the independent transient block must survive: %+v", sonnet)
	}
	fable := auth.ModelStates["claude-fable-5"]
	if !fable.Unavailable || fable.NextRetryAfter.Before(now.Add(71*time.Hour)) {
		t.Fatalf("a model-scoped limit outside the recovery must survive: %+v", fable)
	}

	if _, err = manager.ClearRecoveredQuota(context.Background(), id, QuotaRecovery{ModelQuota: true, Windows: []string{"7d_oi"}}); err != nil {
		t.Fatal(err)
	}
	auth = recoveryState(t, manager, id)
	if fable = auth.ModelStates["claude-fable-5"]; fable.Unavailable || fable.Quota.Exceeded {
		t.Fatalf("a recovered model window must release the model limit: %+v", fable)
	}
	if sonnet = auth.ModelStates["claude-sonnet-5"]; !sonnet.Unavailable {
		t.Fatal("a model recovery must not release a transient block")
	}
}

func TestClearRecoveredQuotaKeepsCredentialQuotaWhenNotRecovered(t *testing.T) {
	const id = "recovery-not-recovered"
	manager := registerRecoveryAuth(t, id, "claude", nil)
	markRecoveryResult(manager, id, "claude-opus-5", http.StatusTooManyRequests, 72*time.Hour, true)
	// A 5h-only grant: the weekly limit that cooled the credential is still exhausted.
	if _, err := manager.ClearRecoveredQuota(context.Background(), id, QuotaRecovery{Windows: []string{"5h"}}); err != nil {
		t.Fatal(err)
	}
	auth := recoveryState(t, manager, id)
	if auth.Quota.Reason != "credential_quota" || !auth.ModelStates["claude-opus-5"].Unavailable {
		t.Fatalf("an unrecovered credential quota must stay: %+v", auth.Quota)
	}
}

func TestClearRecoveredQuotaDropsOnlyRecoveredSignals(t *testing.T) {
	t.Run("claude", func(t *testing.T) {
		const id = "recovery-claude-signals"
		manager := registerRecoveryAuth(t, id, "claude", map[string]string{
			"Anthropic-Ratelimit-Unified-5h-Utilization":    "1.0",
			"Anthropic-Ratelimit-Unified-5h-Reset":          "1793102375",
			"Anthropic-Ratelimit-Unified-7d-Utilization":    "0.4",
			"Anthropic-Ratelimit-Unified-7d-Reset":          "1793500000",
			"Anthropic-Ratelimit-Unified-7d_oi-Utilization": "0.9",
			"Anthropic-Ratelimit-Unified-Status":            "rejected",
			"Retry-After":                                   "600",
		})
		if _, err := manager.ClearRecoveredQuota(context.Background(), id, QuotaRecovery{Windows: []string{"5h"}}); err != nil {
			t.Fatal(err)
		}
		signals := recoveryState(t, manager, id).Quota.Signals
		if _, ok := signals["Anthropic-Ratelimit-Unified-5h-Reset"]; ok {
			t.Fatalf("recovered 5h signals must go: %v", signals)
		}
		for _, kept := range []string{"Anthropic-Ratelimit-Unified-7d-Reset", "Anthropic-Ratelimit-Unified-7d_oi-Utilization", "Anthropic-Ratelimit-Unified-Status", "Retry-After"} {
			if _, ok := signals[kept]; !ok {
				t.Fatalf("%s must stay without a credential release: %v", kept, signals)
			}
		}
		if _, err := manager.ClearRecoveredQuota(context.Background(), id, QuotaRecovery{CredentialQuota: true}); err != nil {
			t.Fatal(err)
		}
		signals = recoveryState(t, manager, id).Quota.Signals
		if _, ok := signals["Anthropic-Ratelimit-Unified-Status"]; ok {
			t.Fatalf("rejection markers go with the credential release: %v", signals)
		}
		if _, ok := signals["Anthropic-Ratelimit-Unified-7d-Reset"]; !ok {
			t.Fatalf("the weekly reset was not recovered and must stay: %v", signals)
		}
	})
	t.Run("codex", func(t *testing.T) {
		const id = "recovery-codex-signals"
		manager := registerRecoveryAuth(t, id, "codex", map[string]string{
			"X-Codex-Primary-Used-Percent":     "100",
			"X-Codex-Primary-Window-Minutes":   "300",
			"X-Codex-Secondary-Used-Percent":   "100",
			"X-Codex-Secondary-Window-Minutes": "10080",
		})
		if _, err := manager.ClearRecoveredQuota(context.Background(), id, QuotaRecovery{Windows: []string{"7d"}}); err != nil {
			t.Fatal(err)
		}
		signals := recoveryState(t, manager, id).Quota.Signals
		if _, ok := signals["X-Codex-Secondary-Used-Percent"]; ok {
			t.Fatalf("the recovered weekly slot must go: %v", signals)
		}
		if _, ok := signals["X-Codex-Primary-Used-Percent"]; !ok {
			t.Fatalf("the 5h slot was not recovered and must stay: %v", signals)
		}
		if _, err := manager.ClearRecoveredQuota(context.Background(), id, QuotaRecovery{Windows: []string{"5h"}}); err != nil {
			t.Fatal(err)
		}
		auth := recoveryState(t, manager, id)
		if auth.Quota.Signals != nil || !auth.Quota.ObservedAt.IsZero() {
			t.Fatalf("with every window recovered the account is unobserved: %+v", auth.Quota)
		}
	})
}
