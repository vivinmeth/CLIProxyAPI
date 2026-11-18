package auth

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/refusals"
	log "github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
)

func TestTMXAccountRefusalDisablesOnce(t *testing.T) {
	for _, tc := range []struct{ name, provider, code, body, reason string }{
		{"meta_code", "meta", "", `{"error":{"code":"client_attestation_denied"}}`, refusals.MetaAttestation},
		{"meta_message", "meta", "", `{"error":{"message":"Muse Code subscriptions cover requests from the Muse Code client only."}}`, refusals.MetaAttestation},
		{"meta_text", "meta", "", "Muse Code subscriptions cover requests from the Muse Code client only.", refusals.MetaAttestation},
		{"meta_direct_code", "meta", "client_attestation_denied", "refused", refusals.MetaAttestation},
		{"upstream_tos", "antigravity", "tos_violation", "TOS_VIOLATION", "tos_violation"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			originalHooks := log.StandardLogger().ReplaceHooks(make(log.LevelHooks))
			t.Cleanup(func() { log.StandardLogger().ReplaceHooks(originalHooks) })
			hook := logtest.NewGlobal()
			for _, model := range []string{"", "model"} {
				hook.Reset()
				ctx := context.Background()
				m := NewManager(nil, nil, nil)
				o := &recordingCooldownObserver{}
				SetCooldownObserver(o)
				defer SetCooldownObserver(nil)
				a := &Auth{ID: "refusal", Provider: tc.provider, Status: StatusActive, NextRefreshAfter: time.Now().Add(time.Hour)}
				if _, err := m.Register(ctx, a); err != nil {
					t.Fatal(err)
				}
				failure := Result{AuthID: a.ID, Provider: tc.provider, Model: model, Error: &Error{HTTPStatus: 403, Code: tc.code, Message: tc.body}}
				m.MarkResult(ctx, failure)
				got, _ := m.GetByID(a.ID)
				if !got.Disabled || got.Status != StatusDisabled || !got.Unavailable || !got.NextRetryAfter.IsZero() || !got.NextRefreshAfter.IsZero() || !strings.Contains(got.StatusMessage, refusals.StatusMessage(tc.reason)) {
					t.Fatalf("terminal state = %+v", got)
				}
				if got.LastError == nil || got.LastError.Code != tc.reason {
					t.Fatalf("reason = %+v", got.LastError)
				}
				for _, future := range []time.Time{time.Now(), time.Now().Add(365 * 24 * time.Hour)} {
					if blocked, _, retry := isAuthBlockedForModel(got, model, future); !blocked || !retry.IsZero() {
						t.Fatal("terminal auth became selectable or has retry deadline")
					}
				}
				m.MarkResult(ctx, failure)
				m.MarkResult(ctx, Result{AuthID: a.ID, Provider: tc.provider, Model: model, Success: true})
				m.MarkResult(ctx, Result{AuthID: a.ID, Provider: tc.provider, Error: &Error{HTTPStatus: 429, Message: "rate limited"}})
				after, _ := m.GetByID(a.ID)
				if !after.Disabled || after.Status != StatusDisabled || after.StatusMessage != got.StatusMessage || !after.NextRetryAfter.IsZero() {
					t.Fatal("late result revived or changed refusal")
				}
				refusalsSeen := 0
				for _, f := range o.failures {
					if f.ErrorCode == tc.reason {
						refusalsSeen++
					}
				}
				if refusalsSeen != 1 {
					t.Fatalf("refusal reports = %d, want 1", refusalsSeen)
				}
				logs := 0
				for _, e := range hook.AllEntries() {
					if strings.HasPrefix(e.Message, "account refusal:") {
						logs++
					}
				}
				if logs != 1 {
					t.Fatalf("refusal logs = %d, want 1", logs)
				}
			}
		})
	}
}

func TestTMXOrdinary403And429KeepCooldown(t *testing.T) {
	for _, status := range []int{403, 429} {
		m := NewManager(nil, nil, nil)
		_, _ = m.Register(context.Background(), &Auth{ID: "ordinary", Provider: "meta", Status: StatusActive})
		before := time.Now()
		m.MarkResult(context.Background(), Result{AuthID: "ordinary", Provider: "meta", Error: &Error{HTTPStatus: status, Message: "permission denied"}})
		a, _ := m.GetByID("ordinary")
		if a.Disabled || a.Status == StatusDisabled || !a.NextRetryAfter.After(before) {
			t.Fatalf("status %d lost cooldown", status)
		}
		if status == 403 && (a.NextRetryAfter.Before(before.Add(30*time.Minute)) || a.NextRetryAfter.After(time.Now().Add(30*time.Minute))) {
			t.Fatal("403 cooldown changed")
		}
		if status == 429 && !a.Quota.Exceeded {
			t.Fatal("429 quota state lost")
		}
	}
}

func TestTMXRefusalHonorsCredentialIdentity(t *testing.T) {
	for _, replaced := range []bool{false, true} {
		m := NewManager(nil, nil, nil)
		_, _ = m.Register(context.Background(), &Auth{ID: "identity", Provider: "meta", Status: StatusActive, CredentialVersion: 2, Metadata: map[string]any{"access_token": "new", "refresh_token": "current"}})
		rt := "current"
		if replaced {
			rt = "previous"
		}
		m.MarkResult(context.Background(), Result{AuthID: "identity", Provider: "meta", CredentialVersion: 1, AccessToken: "old", RefreshToken: rt, Error: &Error{HTTPStatus: 403, Message: `{"error":{"code":"client_attestation_denied"}}`}})
		a, _ := m.GetByID("identity")
		if a.Disabled == replaced {
			t.Fatalf("replaced=%v disabled=%v", replaced, a.Disabled)
		}
	}
}

func TestTMXDisabledAuthNeverRefreshesUntilReenabled(t *testing.T) {
	for _, statusOnly := range []bool{false, true} {
		ctx := context.Background()
		m := NewManager(nil, nil, nil)
		e := &mockOAuthErrorExecutor{id: "disabled-refresh"}
		m.RegisterExecutor(e)
		a := &Auth{ID: "disabled", Provider: e.id, Disabled: !statusOnly, Status: StatusActive, Metadata: map[string]any{"refresh_token": "local-test", "refresh_interval_seconds": 1}}
		if statusOnly {
			a.Status = StatusDisabled
		}
		_, _ = m.Register(ctx, a)
		a, _ = m.GetByID(a.ID)
		now := time.Now()
		if m.shouldRefresh(a, now) {
			t.Fatal("disabled auth wants refresh")
		}
		if _, ok := nextRefreshCheckAt(now, a, refreshCheckInterval); ok {
			t.Fatal("disabled auth scheduled")
		}
		if job := m.markRefreshPending(nil, a.ID, a.RegistrationEpoch, now); job != nil {
			t.Fatal("disabled auth queued")
		}
		for _, refreshCtx := range []context.Context{ctx, withForceRefresh(ctx)} {
			if _, err := m.refreshAuthForRequest(refreshCtx, a.ID, ""); err == nil {
				t.Fatal("disabled direct refresh accepted")
			}
		}
		if e.refreshCalls.Load() != 0 {
			t.Fatal("disabled auth reached executor")
		}
		a.Disabled = false
		a.Status = StatusActive
		if _, err := m.Update(ctx, a); err != nil {
			t.Fatal(err)
		}
		a, _ = m.GetByID(a.ID)
		if !m.shouldRefresh(a, now) {
			t.Fatal("re-enabled auth cannot refresh")
		}
		job := m.markRefreshPending(nil, a.ID, a.RegistrationEpoch, now)
		if job == nil || !m.beginRefreshJob(ctx, job) {
			t.Fatal("re-enabled auth cannot start")
		}
		if _, err := m.refreshAuthForRequestAtEpoch(ctx, a.ID, "", job.registrationEpoch); err != nil {
			t.Fatal(err)
		}
		m.finishRefreshJob(job, time.Time{}, false)
		if e.refreshCalls.Load() != 1 {
			t.Fatal("re-enable did not refresh once")
		}
	}
}

func TestTMXQueuedRefreshCannotStartAfterDisable(t *testing.T) {
	m := NewManager(nil, nil, nil)
	ctx := context.Background()
	a, _ := m.Register(ctx, &Auth{ID: "queued", Provider: "meta", Status: StatusActive})
	job := m.markRefreshPending(nil, a.ID, a.RegistrationEpoch, time.Now())
	if job == nil {
		t.Fatal("missing job")
	}
	a, _ = m.GetByID(a.ID)
	a.Disabled = true
	a.Status = StatusDisabled
	_, _ = m.Update(ctx, a)
	if m.beginRefreshJob(ctx, job) {
		t.Fatal("queued refresh started after disable")
	}
	m.finishRefreshJob(job, time.Time{}, true)
}

// A refresh already in flight may finish, but cannot remove the refusal reason.
type tmxBlockedRefreshExecutor struct {
	mockOAuthErrorExecutor
	started, release chan struct{}
	failure          bool
}

func (e *tmxBlockedRefreshExecutor) Refresh(_ context.Context, a *Auth) (*Auth, error) {
	e.refreshCalls.Add(1)
	close(e.started)
	<-e.release
	if e.failure {
		return nil, oauthStatusError{code: 400, msg: "invalid_grant"}
	}
	a.Metadata["access_token"] = "rotated-before-disable"
	return a, nil
}

func TestTMXInflightRefreshPreservesAccountRefusal(t *testing.T) {
	for _, failure := range []bool{false, true} {
		ctx := context.Background()
		m := NewManager(nil, nil, nil)
		e := &tmxBlockedRefreshExecutor{mockOAuthErrorExecutor: mockOAuthErrorExecutor{id: "meta"}, started: make(chan struct{}), release: make(chan struct{}), failure: failure}
		m.RegisterExecutor(e)
		_, _ = m.Register(ctx, &Auth{ID: "inflight", Provider: "meta", Status: StatusActive, Metadata: map[string]any{"access_token": "before", "refresh_token": "local-test"}})
		done := make(chan struct{})
		go func() { defer close(done); _, _ = m.refreshAuthForRequest(ctx, "inflight", "") }()
		<-e.started
		m.MarkResult(ctx, Result{AuthID: "inflight", Provider: "meta", Error: &Error{HTTPStatus: 403, Message: `{"error":{"code":"client_attestation_denied"}}`}})
		close(e.release)
		<-done
		a, _ := m.GetByID("inflight")
		if !a.Disabled || a.Status != StatusDisabled || a.LastError == nil || a.LastError.Code != refusals.MetaAttestation || a.StatusMessage != refusals.StatusMessage(refusals.MetaAttestation) {
			t.Fatalf("inflight failure=%v lost refusal: %+v", failure, a)
		}
		if _, ok := m.nextRefreshCheckAt(time.Now(), a, time.Second); ok {
			t.Fatal("inflight refresh rescheduled disabled credential")
		}
		if !failure && a.Metadata["access_token"] != "rotated-before-disable" {
			t.Fatal("completed token rotation lost")
		}
	}
}

func TestTMXAccountRefusalSurvivesMetadataReload(t *testing.T) {
	for _, code := range []string{refusals.MetaAttestation, "tos_violation"} {
		for _, load := range []bool{false, true} {
			ctx := context.Background()
			m := NewManager(nil, nil, nil)
			provider := "meta"
			rawCode := "client_attestation_denied"
			if code == "tos_violation" {
				provider = "antigravity"
				rawCode = code
			}
			_, _ = m.Register(ctx, &Auth{ID: "persisted", Provider: provider, Status: StatusActive})
			m.MarkResult(ctx, Result{AuthID: "persisted", Provider: provider, Error: &Error{HTTPStatus: 403, Code: rawCode}})
			disabled, _ := m.GetByID("persisted")
			// Token stores reload metadata and the disabled flag, without runtime errors/status messages.
			fixture := &Auth{ID: disabled.ID, Provider: provider, Disabled: true, Status: StatusDisabled, Metadata: disabled.Clone().Metadata}
			reloaded := NewManager(nil, nil, nil)
			if load {
				store := &reloadAuthStore{records: map[string]*Auth{fixture.ID: fixture}, listEntered: make(chan struct{})}
				reloaded = NewManager(store, nil, nil)
				if err := reloaded.Load(ctx); err != nil {
					t.Fatal(err)
				}
			} else if _, err := reloaded.Register(ctx, fixture); err != nil {
				t.Fatal(err)
			}
			a, _ := reloaded.GetByID(fixture.ID)
			if !a.Disabled || a.LastError == nil || a.LastError.Code != code || a.StatusMessage != refusals.StatusMessage(code) {
				t.Fatalf("load=%v refusal lost: %+v", load, a)
			}
			a.Disabled = false
			a.Status = StatusActive
			if _, err := reloaded.Update(ctx, a); err != nil {
				t.Fatal(err)
			}
			a, _ = reloaded.GetByID(a.ID)
			if a.Metadata[tmxRefusalMetadataKey] != nil || tmxTerminalRefusal(a.LastError) {
				t.Fatal("explicit re-enable retained terminal marker")
			}
		}
	}
}
