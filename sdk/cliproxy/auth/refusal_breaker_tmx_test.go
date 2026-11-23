package auth

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/refusals"
	log "github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
)

func tmxMetaBreakerFixture(t *testing.T, store Store) (*Manager, *time.Time, *logtest.Hook) {
	t.Helper()
	original := log.StandardLogger().ReplaceHooks(make(log.LevelHooks))
	t.Cleanup(func() { log.StandardLogger().ReplaceHooks(original) })
	hook := logtest.NewGlobal()
	m := NewManager(store, nil, nil)
	now := time.Date(2026, 11, 23, 0, 0, 0, 0, time.UTC)
	m.tmxMetaRefusal.now = func() time.Time { return now }
	return m, &now, hook
}

func tmxMetaBreakerAccount(t *testing.T, m *Manager, id string) *Auth {
	t.Helper()
	a, err := m.Register(context.Background(), &Auth{ID: id, Provider: "meta", Status: StatusActive, Metadata: map[string]any{"access_token": "synthetic-" + id, "refresh_token": "synthetic-refresh-" + id}})
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func tmxMetaBreakerResult(a *Auth, model string) Result {
	return Result{AuthID: a.ID, Provider: a.Provider, Model: model, RegistrationEpoch: a.RegistrationEpoch, CredentialVersion: a.CredentialVersion,
		AccessToken: authAccessToken(a), RefreshToken: authRefreshToken(a), Error: &Error{HTTPStatus: 403, Code: "client_attestation_denied", Message: "synthetic-private-response"}}
}

func tmxMetaBreakerAlerts(hook *logtest.Hook) int {
	n := 0
	for _, entry := range hook.AllEntries() {
		if entry.Message == refusals.MetaBreakerAlert {
			n++
		}
	}
	return n
}

func TestTMXMetaBreakerCapCooldownAndQuietRearm(t *testing.T) {
	for _, model := range []string{"", "model"} {
		t.Run("scope="+model, func(t *testing.T) {
			store := newTMXRefusalStore()
			m, now, hook := tmxMetaBreakerFixture(t, store)
			accounts := make([]*Auth, 5)
			for i := range accounts {
				accounts[i] = tmxMetaBreakerAccount(t, m, fmt.Sprint(i))
			}
			for i := 0; i < 2; i++ {
				m.MarkResult(context.Background(), tmxMetaBreakerResult(accounts[i], model))
				got, _ := m.GetByID(accounts[i].ID)
				if !got.Disabled || got.Metadata[tmxRefusalMetadataKey] != refusals.MetaAttestation {
					t.Fatal("first two must be disabled")
				}
				// Already disabled results neither consume slots nor repeat the alert.
				for j := 0; j < 3; j++ {
					m.MarkResult(context.Background(), tmxMetaBreakerResult(accounts[i], model))
				}
				if got := m.TmxMetaRefusalBreakerStatus().DisablesInWindow; got != i+1 {
					t.Fatalf("disables=%d", got)
				}
			}
			if tmxMetaBreakerAlerts(hook) != 1 {
				t.Fatal("expected one safe alert at cap")
			}
			*now = now.Add(59 * time.Minute)
			before := time.Now()
			m.MarkResult(context.Background(), tmxMetaBreakerResult(accounts[2], model))
			third, _ := m.GetByID(accounts[2].ID)
			if third.Disabled || third.Status == StatusDisabled || third.Metadata[tmxRefusalMetadataKey] != nil || third.LastError.Code != refusals.AttestationRefused {
				t.Fatal("third became terminal")
			}
			if blocked, _, retry := isAuthBlockedForModel(third, model, before); !blocked || retry.Before(before.Add(30*time.Minute)) {
				t.Fatal("third lost ordinary cooldown")
			}
			stored := store.records[third.ID]
			if stored.Disabled || stored.Metadata[tmxRefusalMetadataKey] != nil {
				t.Fatal("terminal state was persisted before demotion")
			}
			status := m.TmxMetaRefusalBreakerStatus()
			if !status.Tripped || status.Alert != refusals.MetaBreakerAlert || !status.QuietUntil.Equal(now.Add(time.Hour)) {
				t.Fatalf("status=%+v", status)
			}
			*now = now.Add(59 * time.Minute)
			m.MarkResult(context.Background(), tmxMetaBreakerResult(accounts[3], model))
			fourth, _ := m.GetByID(accounts[3].ID)
			if fourth.Disabled || !m.TmxMetaRefusalBreakerStatus().Tripped {
				t.Fatal("sliding count expiry rearmed during ongoing refusals")
			}
			if tmxMetaBreakerAlerts(hook) != 1 {
				t.Fatal("alert repeated within trip")
			}
			*now = now.Add(time.Hour)
			if m.TmxMetaRefusalBreakerStatus().Tripped {
				t.Fatal("full quiet window did not rearm")
			}
			m.MarkResult(context.Background(), tmxMetaBreakerResult(accounts[4], model))
			last, _ := m.GetByID(accounts[4].ID)
			if !last.Disabled {
				t.Fatal("rearm did not restore disable path")
			}
			for _, a := range accounts[:2] {
				got, _ := m.GetByID(a.ID)
				if !got.Disabled || got.Metadata[tmxRefusalMetadataKey] != refusals.MetaAttestation {
					t.Fatal("rearm cleared a stored refusal")
				}
			}
			for _, entry := range hook.AllEntries() {
				if strings.Contains(entry.Message, "synthetic-private") || strings.Contains(entry.Message, "synthetic-refresh") {
					t.Fatal("unsafe operator log")
				}
			}
		})
	}
}

func TestTMXMetaBreakerConcurrentDistinctAccounts(t *testing.T) {
	m, _, hook := tmxMetaBreakerFixture(t, nil)
	const n = 8
	accounts := make([]*Auth, n)
	for i := range accounts {
		accounts[i] = tmxMetaBreakerAccount(t, m, fmt.Sprint(i))
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, a := range accounts {
		wg.Add(1)
		go func() { defer wg.Done(); <-start; m.MarkResult(context.Background(), tmxMetaBreakerResult(a, "model")) }()
	}
	close(start)
	wg.Wait()
	disabled, cooling := 0, 0
	for _, a := range m.List() {
		if a.Disabled {
			disabled++
		} else if a.LastError != nil && a.LastError.Code == refusals.AttestationRefused {
			cooling++
		}
	}
	if disabled != 2 || cooling != n-2 || tmxMetaBreakerAlerts(hook) != 1 {
		t.Fatalf("disabled=%d cooling=%d alerts=%d", disabled, cooling, tmxMetaBreakerAlerts(hook))
	}
}

func TestTMXMetaBreakerStaleAndUnaffectedResults(t *testing.T) {
	m, _, _ := tmxMetaBreakerFixture(t, nil)
	a := tmxMetaBreakerAccount(t, m, "stale")
	for _, kind := range []string{"epoch", "grant", "tools", "ordinary", "xai", "google", "claude"} {
		result := tmxMetaBreakerResult(a, "model")
		switch kind {
		case "epoch": // The registered epoch below supersedes this snapshot.
		case "grant":
			result.CredentialVersion = a.CredentialVersion
			result.AccessToken = "replaced"
			result.RefreshToken = "replaced"
		case "tools":
			result.Error.Code = refusals.AttestationRefused
		case "ordinary":
			result.Error = &Error{HTTPStatus: 403, Message: "generic permission error"}
		default:
			provider := kind
			if kind == "google" {
				provider = "antigravity"
			}
			other, err := m.Register(context.Background(), &Auth{ID: kind, Provider: provider, Status: StatusActive})
			if err != nil {
				t.Fatal(err)
			}
			result = tmxMetaBreakerResult(other, "model")
			if kind == "google" {
				result.Error.Code = refusals.GoogleValidation
			}
		}
		if kind == "epoch" || kind == "grant" {
			// Supersede the registration/version to exercise the real stale gate.
			m.mu.Lock()
			m.auths[a.ID].RegistrationEpoch = a.RegistrationEpoch + 1
			if kind == "grant" {
				m.auths[a.ID].RegistrationEpoch = a.RegistrationEpoch
				m.auths[a.ID].CredentialVersion = a.CredentialVersion + 1
			}
			m.mu.Unlock()
		} else {
			result.RegistrationEpoch = 0
			result.CredentialVersion = 0
		}
		m.MarkResult(context.Background(), result)
		if got := m.TmxMetaRefusalBreakerStatus(); got.DisablesInWindow != 0 || got.Tripped {
			t.Fatalf("%s consumed breaker slot: %+v", kind, got)
		}
		m.mu.Lock()
		m.auths[a.ID].RegistrationEpoch = a.RegistrationEpoch
		m.auths[a.ID].CredentialVersion = a.CredentialVersion
		m.mu.Unlock()
	}
	google, _ := m.GetByID("google")
	if !google.Disabled || google.LastError.Code != refusals.GoogleValidation {
		t.Fatal("Google refusal was demoted")
	}
	for _, id := range []string{"xai", "claude"} {
		got, _ := m.GetByID(id)
		if got.Disabled {
			t.Fatalf("%s became disabled", id)
		}
	}
}

func TestTMXMetaBreakerPersistenceFailureStillConsumesCap(t *testing.T) {
	store := newTMXRefusalStore()
	m, _, _ := tmxMetaBreakerFixture(t, store)
	accounts := []*Auth{tmxMetaBreakerAccount(t, m, "a"), tmxMetaBreakerAccount(t, m, "b"), tmxMetaBreakerAccount(t, m, "c")}
	store.fail = true
	for _, a := range accounts {
		m.MarkResult(context.Background(), tmxMetaBreakerResult(a, ""))
	}
	if got := m.TmxMetaRefusalBreakerStatus(); !got.Tripped || got.DisablesInWindow != 2 {
		t.Fatalf("status=%+v", got)
	}
	third, _ := m.GetByID("c")
	if third.Disabled || third.LastError.Code != refusals.AttestationRefused {
		t.Fatal("failed persistence bypassed cap")
	}
}

func TestTMXMetaBreakerConfiguredCapAndReload(t *testing.T) {
	m, now, hook := tmxMetaBreakerFixture(t, nil)
	m.SetConfig(&internalconfig.Config{TMXMetaRefusalBreaker: internalconfig.MetaRefusalBreakerConfig{Threshold: 1, WindowSeconds: 60}})
	a := tmxMetaBreakerAccount(t, m, "a")
	b := tmxMetaBreakerAccount(t, m, "b")
	m.MarkResult(context.Background(), tmxMetaBreakerResult(a, "model"))
	// An ordinary config reload must not erase the trip.
	m.SetConfig(&internalconfig.Config{TMXMetaRefusalBreaker: internalconfig.MetaRefusalBreakerConfig{Threshold: 1, WindowSeconds: 60}})
	m.MarkResult(context.Background(), tmxMetaBreakerResult(b, "model"))
	got, _ := m.GetByID(b.ID)
	if got.Disabled || !m.TmxMetaRefusalBreakerStatus().Tripped {
		t.Fatal("reload bypassed configured cap")
	}
	*now = now.Add(time.Minute)
	m.MarkResult(context.Background(), tmxMetaBreakerResult(b, "model"))
	got, _ = m.GetByID(b.ID)
	if !got.Disabled || tmxMetaBreakerAlerts(hook) != 2 {
		t.Fatal("new trip after quiet window failed")
	}
}
