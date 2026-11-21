package auth

import (
	"context"
	"errors"
	"fmt"
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
		{"google_validation", "antigravity", refusals.GoogleValidation, "ignored", refusals.GoogleValidation},
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

type tmxRefusalStore struct {
	*reloadAuthStore
	saves int
	fail  bool
	skip  bool
}

func (s *tmxRefusalStore) Save(ctx context.Context, a *Auth) (string, error) {
	s.saves++
	if s.skip {
		return "", nil
	}
	if s.fail {
		return "", errors.New("synthetic-store-failure https://example.invalid/session-private?token=synthetic-secret")
	}
	return s.reloadAuthStore.Save(ctx, a)
}

func newTMXRefusalStore() *tmxRefusalStore {
	return &tmxRefusalStore{reloadAuthStore: &reloadAuthStore{records: make(map[string]*Auth), listEntered: make(chan struct{})}}
}

func tmxRefuseFixture(t *testing.T, m *Manager) *Auth {
	t.Helper()
	a, err := m.Register(context.Background(), &Auth{ID: "lifecycle", Provider: "antigravity", Status: StatusActive, Metadata: map[string]any{"access_token": "synthetic-old", "refresh_token": "synthetic-refresh", "note": "old"}})
	if err != nil {
		t.Fatal(err)
	}
	m.MarkResult(context.Background(), Result{AuthID: a.ID, Provider: a.Provider, Model: "model", Error: &Error{HTTPStatus: 403, Message: tmxGoogleValidationBody}})
	return a
}

func assertTMXRefused(t *testing.T, a *Auth) {
	t.Helper()
	if a == nil || !a.Disabled || a.Status != StatusDisabled || !a.Unavailable || a.LastError == nil || a.LastError.Code != refusals.GoogleValidation || a.Metadata[tmxRefusalMetadataKey] != refusals.GoogleValidation || a.Metadata["disabled"] != true || !a.NextRetryAfter.IsZero() || !a.NextRefreshAfter.IsZero() {
		t.Fatalf("refusal lost: %+v", a)
	}
	assertTMXNoValidationData(t, fmt.Sprintf("%+v", a.LastError))
}

func TestTMXRefusalLifecycleOwnsState(t *testing.T) {
	for _, mode := range []string{"stale", "fresh", "marker-enabled", "refresh", "prepare", "register"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			store := newTMXRefusalStore()
			m := NewManager(store, nil, nil)
			stale := tmxRefuseFixture(t, m)
			incoming := stale.Clone()
			if mode == "fresh" {
				incoming = &Auth{ID: stale.ID, Provider: stale.Provider, Status: StatusActive, Metadata: map[string]any{}}
			}
			if mode == "marker-enabled" {
				incoming, _ = m.GetByID(stale.ID)
				incoming.Disabled = false
				incoming.Status = StatusActive
				incoming.Metadata["disabled"] = false
			}
			incoming.Metadata["access_token"] = "synthetic-new"
			incoming.Metadata["note"] = "new"
			var err error
			switch mode {
			case "refresh":
				_, err = m.UpdateRefreshedAuth(ctx, stale, incoming)
			case "prepare":
				_, err = m.UpdatePreparedAuth(ctx, stale, incoming)
			case "register":
				_, err = m.Register(ctx, incoming)
			default:
				_, err = m.Update(ctx, incoming)
			}
			if err != nil {
				t.Fatal(err)
			}
			got, _ := m.GetByID(stale.ID)
			assertTMXRefused(t, got)
			if got.Metadata["access_token"] != "synthetic-new" || got.Metadata["note"] != "new" {
				t.Fatal("incoming token or unrelated metadata lost")
			}
			if mode == "register" && got.RegistrationEpoch <= stale.RegistrationEpoch {
				t.Fatal("Register did not advance epoch")
			}
		})
	}
}

func TestTMXRefusalReloadAuthoritativeAndRepairsStore(t *testing.T) {
	for _, existing := range []bool{false, true} {
		for _, skip := range []bool{false, true} {
			t.Run(fmt.Sprintf("existing=%v/skip=%v", existing, skip), func(t *testing.T) {
				ctx := context.Background()
				store := newTMXRefusalStore()
				m := NewManager(store, nil, nil)
				if existing {
					tmxRefuseFixture(t, m)
				}
				metadata := map[string]any{"disabled": false, "access_token": "synthetic-new"}
				if !existing {
					metadata[tmxRefusalMetadataKey] = refusals.GoogleValidation
				}
				fixture := &Auth{ID: "lifecycle", Provider: "antigravity", Status: StatusActive, Metadata: metadata}
				store.records[fixture.ID] = fixture
				if skip {
					ctx = WithSkipPersist(ctx)
				}
				if err := m.Load(ctx); err != nil {
					t.Fatal(err)
				}
				got, _ := m.GetByID(fixture.ID)
				assertTMXRefused(t, got)
				if existing && store.records[fixture.ID].Metadata[tmxRefusalMetadataKey] != refusals.GoogleValidation {
					t.Fatal("reload did not write back preserved refusal")
				}
			})
		}
	}
}

func TestTMXRefusalSkipPersistRepairsOnce(t *testing.T) {
	for _, register := range []bool{false, true} {
		store := newTMXRefusalStore()
		m := NewManager(store, nil, nil)
		stale := tmxRefuseFixture(t, m)
		before := store.saves
		var err error
		if register {
			_, err = m.Register(WithSkipPersist(context.Background()), stale)
		} else {
			_, err = m.Update(WithSkipPersist(context.Background()), stale)
		}
		if err != nil {
			t.Fatal(err)
		}
		if store.saves != before+1 || store.records[stale.ID].Metadata[tmxRefusalMetadataKey] != refusals.GoogleValidation {
			t.Fatal("watcher update was not repaired in store")
		}
		got, _ := m.GetByID(stale.ID)
		assertTMXRefused(t, got)
		before = store.saves
		if _, err = m.Update(WithSkipPersist(context.Background()), got); err != nil {
			t.Fatal(err)
		}
		if store.saves != before {
			t.Fatal("marked watcher input causes a write loop")
		}
	}
}

func TestTMXRefusalCleanRegistrationNeedsRemove(t *testing.T) {
	ctx := context.Background()
	m := NewManager(nil, nil, nil)
	old := tmxRefuseFixture(t, m)
	m.Remove(ctx, old.ID)
	clean, err := m.Register(ctx, &Auth{ID: old.ID, Provider: old.Provider, Status: StatusActive})
	if err != nil {
		t.Fatal(err)
	}
	if clean.Disabled || clean.LastError != nil || clean.Metadata[tmxRefusalMetadataKey] != nil {
		t.Fatal("clean registration refused")
	}
	m.MarkResult(ctx, Result{AuthID: old.ID, Provider: old.Provider, RegistrationEpoch: old.RegistrationEpoch, Error: &Error{HTTPStatus: 403, Code: refusals.GoogleValidation}})
	got, _ := m.GetByID(old.ID)
	if got.Disabled {
		t.Fatal("old epoch disabled clean registration")
	}
}

func TestTMXRefusalExplicitClearIntent(t *testing.T) {
	ctx := context.Background()
	if hasAccountRefusalClear(nil) || hasAccountRefusalClear(ctx) || !hasAccountRefusalClear(WithAccountRefusalClear(nil)) {
		t.Fatal("clear intent context contract failed")
	}
	store := newTMXRefusalStore()
	m := NewManager(store, nil, nil)
	stale := tmxRefuseFixture(t, m)
	got, ok, err := m.Modify(WithAccountRefusalClear(ctx), stale.ID, stale.RegistrationEpoch, func(current *Auth) error { current.Disabled = false; current.Status = StatusActive; return nil })
	if err != nil || !ok {
		t.Fatalf("explicit clear failed: %v", err)
	}
	if got.Disabled || got.LastError != nil || got.Metadata[tmxRefusalMetadataKey] != nil || got.Metadata["disabled"] != false || got.Unavailable || got.StatusMessage != "" || !modelStateIsClean(got.ModelStates["model"]) {
		t.Fatalf("explicit clear retained refusal: %+v", got)
	}
	if store.records[got.ID].Disabled || store.records[got.ID].Metadata[tmxRefusalMetadataKey] != nil {
		t.Fatal("clear not persisted")
	}
	if next, ok := m.nextRefreshCheckAt(time.Now(), got, refreshCheckInterval); !ok || next.After(time.Now()) {
		t.Fatal("clear did not resume refresh")
	}
	// Intent cannot clear a record that the operator still requests disabled.
	m.MarkResult(ctx, Result{AuthID: got.ID, Provider: got.Provider, Error: &Error{HTTPStatus: 403, Message: tmxGoogleValidationBody}})
	_, _, err = m.Modify(WithAccountRefusalClear(ctx), got.ID, got.RegistrationEpoch, func(current *Auth) error { current.Disabled = true; current.Status = StatusDisabled; return nil })
	if err != nil {
		t.Fatal(err)
	}
	got, _ = m.GetByID(got.ID)
	assertTMXRefused(t, got)
}

func TestTMXRefusalPersistedMarkerOverridesEnabledRegister(t *testing.T) {
	m := NewManager(nil, nil, nil)
	a, err := m.Register(context.Background(), &Auth{ID: "from-store", Provider: "antigravity", Status: StatusActive, Metadata: map[string]any{"disabled": false, tmxRefusalMetadataKey: refusals.GoogleValidation}})
	if err != nil {
		t.Fatal(err)
	}
	assertTMXRefused(t, a)
}

func TestTMXRefusalLateResultsAndOrdinaryDisable(t *testing.T) {
	ctx := context.Background()
	m := NewManager(nil, nil, nil)
	a := tmxRefuseFixture(t, m)
	for _, res := range []Result{{AuthID: a.ID, Provider: a.Provider, Success: true}, {AuthID: a.ID, Provider: a.Provider, Error: &Error{HTTPStatus: 429, Message: "rate limited"}}, {AuthID: a.ID, Provider: a.Provider, Error: &Error{HTTPStatus: 500, Message: "failed"}}} {
		m.MarkResult(ctx, res)
		got, _ := m.GetByID(a.ID)
		assertTMXRefused(t, got)
	}
	ordinary, err := m.Register(ctx, &Auth{ID: "operator", Provider: "antigravity", Disabled: true, Status: StatusDisabled, Metadata: map[string]any{"disabled": true}})
	if err != nil {
		t.Fatal(err)
	}
	ordinary.Disabled = false
	ordinary.Status = StatusActive
	ordinary.Metadata["disabled"] = false
	if _, err = m.Update(ctx, ordinary); err != nil {
		t.Fatal(err)
	}
	got, _ := m.GetByID(ordinary.ID)
	if got.Disabled || got.Status != StatusActive {
		t.Fatal("ordinary operator-disable behavior changed")
	}
}

func TestTMXRefusalPersistenceIsHonest(t *testing.T) {
	oldHooks := log.StandardLogger().ReplaceHooks(make(log.LevelHooks))
	t.Cleanup(func() { log.StandardLogger().ReplaceHooks(oldHooks) })
	hook := logtest.NewGlobal()
	for _, kind := range []string{"saved", "error", "empty-save", "nil-store", "runtime-only", "plugin-virtual", "skip"} {
		t.Run(kind, func(t *testing.T) {
			hook.Reset()
			store := newTMXRefusalStore()
			m := NewManager(store, nil, nil)
			if kind == "nil-store" {
				m = NewManager(nil, nil, nil)
			}
			a := &Auth{ID: "persist", Provider: "antigravity", Status: StatusActive, Metadata: map[string]any{}}
			if kind == "runtime-only" {
				a.Attributes = map[string]string{"runtime_only": "true"}
			}
			if kind == "plugin-virtual" {
				a.Attributes = map[string]string{AttributePluginVirtual: "true"}
			}
			_, _ = m.Register(context.Background(), a)
			store.fail = kind == "error"
			store.skip = kind == "empty-save"
			o := &recordingCooldownObserver{}
			SetCooldownObserver(o)
			t.Cleanup(func() { SetCooldownObserver(nil) })
			ctx := context.Background()
			if kind == "skip" {
				ctx = WithSkipPersist(ctx)
			}
			failure := Result{AuthID: a.ID, Provider: a.Provider, Error: &Error{HTTPStatus: 403, Message: tmxGoogleValidationBody}}
			m.MarkResult(ctx, failure)
			m.MarkResult(ctx, failure)
			got, _ := m.GetByID(a.ID)
			if got == nil || !got.Disabled {
				t.Fatal("memory disable lost")
			}
			memoryOnly := kind != "saved"
			logs := 0
			for _, entry := range hook.AllEntries() {
				assertTMXNoValidationData(t, fmt.Sprintf("%s %+v", entry.Message, entry.Data))
				if strings.HasPrefix(entry.Message, "account refusal:") {
					logs++
					if strings.Contains(entry.Message, "memory-only: not persisted") != memoryOnly || (memoryOnly && entry.Level != log.ErrorLevel) {
						t.Fatalf("dishonest persistence log: %s", entry.Message)
					}
				}
			}
			if logs != 1 || len(o.failures) != 1 || strings.Contains(o.failures[0].Message, "memory-only: not persisted") != memoryOnly {
				t.Fatalf("missing persistence note: logs=%d events=%+v", logs, o.failures)
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

func TestTMXToolAttestationKeepsCooldownAndSibling(t *testing.T) {
	originalHooks := log.StandardLogger().ReplaceHooks(make(log.LevelHooks))
	t.Cleanup(func() { log.StandardLogger().ReplaceHooks(originalHooks) })
	hook := logtest.NewGlobal()
	for _, model := range []string{"", "model"} {
		hook.Reset()
		ctx := context.Background()
		m := NewManager(nil, nil, nil)
		o := &recordingCooldownObserver{}
		SetCooldownObserver(o)
		t.Cleanup(func() { SetCooldownObserver(nil) })
		for _, id := range []string{"tools", "sibling"} {
			if _, err := m.Register(ctx, &Auth{ID: id, Provider: "meta", Status: StatusActive}); err != nil {
				t.Fatal(err)
			}
		}
		before := time.Now()
		failure := Result{AuthID: "tools", Provider: "meta", Model: model, Error: &Error{HTTPStatus: 403, Code: refusals.AttestationRefused, Message: `{"error":{"code":"client_attestation_denied"}}`}}
		m.MarkResult(ctx, failure)
		m.MarkResult(ctx, failure)
		a, _ := m.GetByID("tools")
		if a.Disabled || a.Status == StatusDisabled || a.LastError.Code != refusals.AttestationRefused {
			t.Fatalf("tool refusal became terminal: %+v", a)
		}
		if blocked, _, retry := isAuthBlockedForModel(a, model, time.Now()); !blocked || retry.Before(before.Add(30*time.Minute)) || retry.After(time.Now().Add(30*time.Minute)) {
			t.Fatalf("ordinary 403 cooldown lost: blocked=%v retry=%v", blocked, retry)
		}
		sibling, _ := m.GetByID("sibling")
		if blocked, _, _ := isAuthBlockedForModel(sibling, model, time.Now()); blocked || sibling.Disabled {
			t.Fatal("sibling credential blocked")
		}
		windows := m.CooldownSnapshot().Windows
		if len(windows) == 0 {
			t.Fatal("missing cooldown history windows")
		}
		for _, w := range windows {
			if w.Reason != refusals.AttestationRefused || w.ErrorCode != refusals.AttestationRefused {
				t.Fatalf("wrong history reason: %+v", w)
			}
		}
		if len(o.failures) != 2 || o.failures[0].ErrorCode != refusals.AttestationRefused || o.failures[0].CredentialScope {
			t.Fatalf("tool refusal reports: %+v", o.failures)
		}
		logs := 0
		for _, entry := range hook.AllEntries() {
			if strings.HasPrefix(entry.Message, "attestation refusal:") {
				logs++
			}
		}
		if logs != 1 {
			t.Fatalf("transition logged %d times", logs)
		}
	}
}

func TestTMXRefusalReenableRestoresModelAndRefresh(t *testing.T) {
	for _, code := range []string{refusals.MetaAttestation, "tos_violation", refusals.GoogleValidation} {
		for _, legacy := range []bool{false, true} {
			ctx := context.Background()
			m := NewManager(nil, nil, nil)
			e := &mockOAuthErrorExecutor{id: "meta"}
			if code != refusals.MetaAttestation {
				e.id = "antigravity"
			}
			m.RegisterExecutor(e)
			a, err := m.Register(ctx, &Auth{ID: "reenable", Provider: e.id, Status: StatusActive, Metadata: map[string]any{"refresh_token": "local-test", "refresh_interval_seconds": 3600}})
			if err != nil {
				t.Fatal(err)
			}
			m.MarkResult(ctx, Result{AuthID: a.ID, Provider: e.id, Model: "model", Error: &Error{HTTPStatus: 403, Code: code, Metadata: map[string]string{"tos_appeal_url": "https://example.invalid/appeal?secret=hidden", "tos_appeal_url_text": "appeal"}}})
			a, _ = m.GetByID(a.ID)
			if !a.Disabled || a.ModelStates["model"].Status != StatusDisabled {
				t.Fatal("refusal did not disable auth and model")
			}
			if legacy {
				delete(a.Metadata, tmxRefusalMetadataKey)
			}
			a.Disabled = false
			a.Status = StatusActive
			if _, err := m.Update(WithAccountRefusalClear(ctx), a); err != nil {
				t.Fatal(err)
			}
			a, _ = m.GetByID(a.ID)
			if a.LastError != nil || a.StatusMessage != "" || a.Metadata[tmxRefusalMetadataKey] != nil || a.Metadata["tos_appeal_url"] != nil || a.Metadata["tos_appeal_url_text"] != nil || !modelStateIsClean(a.ModelStates["model"]) {
				t.Fatalf("code=%s legacy=%v retained refusal: %+v model=%+v", code, legacy, a, a.ModelStates["model"])
			}
			now := time.Now()
			for _, model := range []string{"", "model"} {
				if blocked, _, _ := isAuthBlockedForModel(a, model, now); blocked {
					t.Fatalf("re-enabled auth/model %q not selectable", model)
				}
			}
			if next, ok := m.nextRefreshCheckAt(now, a, refreshCheckInterval); !ok || next.After(now) || !m.shouldRefresh(a, now) {
				t.Fatal("re-enable did not schedule immediate refresh")
			}
			job := m.markRefreshPending(nil, a.ID, a.RegistrationEpoch, now)
			if job == nil || m.markRefreshPending(nil, a.ID, a.RegistrationEpoch, now) != nil || !m.beginRefreshJob(ctx, job) {
				t.Fatal("refresh not queued exactly once")
			}
			if _, err := m.refreshAuthForRequestAtEpoch(ctx, a.ID, "", job.registrationEpoch); err != nil {
				t.Fatal(err)
			}
			m.finishRefreshJob(job, time.Time{}, false)
			a, _ = m.GetByID(a.ID)
			if e.refreshCalls.Load() != 1 || m.shouldRefresh(a, time.Now()) {
				t.Fatal("re-enable did not refresh exactly once")
			}
		}
	}
}

func TestTMXRefusalLogRedactsAppeal(t *testing.T) {
	originalHooks := log.StandardLogger().ReplaceHooks(make(log.LevelHooks))
	t.Cleanup(func() { log.StandardLogger().ReplaceHooks(originalHooks) })
	hook := logtest.NewGlobal()
	m := NewManager(nil, nil, nil)
	ctx := context.Background()
	_, _ = m.Register(ctx, &Auth{ID: "appeal", Provider: "antigravity", Status: StatusActive})
	failure := Result{AuthID: "appeal", Provider: "antigravity", Error: &Error{HTTPStatus: 403, Code: "tos_violation", Metadata: map[string]string{"tos_appeal_url": "https://example.invalid/appeal?signed=secret#private"}}}
	m.MarkResult(ctx, failure)
	m.MarkResult(ctx, failure)
	logs := 0
	for _, entry := range hook.AllEntries() {
		if strings.HasPrefix(entry.Message, "account refusal:") {
			logs++
			if !strings.Contains(entry.Message, "appeal=https://example.invalid/appeal") || strings.Contains(entry.Message, "secret") || strings.Contains(entry.Message, "private") {
				t.Fatalf("appeal log not redacted: %s", entry.Message)
			}
		}
	}
	if logs != 1 {
		t.Fatalf("refusal logged %d times", logs)
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
	for _, code := range []string{refusals.MetaAttestation, "tos_violation", refusals.GoogleValidation} {
		for _, load := range []bool{false, true} {
			ctx := context.Background()
			m := NewManager(nil, nil, nil)
			provider := "meta"
			rawCode := "client_attestation_denied"
			if code != refusals.MetaAttestation {
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
			if _, err := reloaded.Update(WithAccountRefusalClear(ctx), a); err != nil {
				t.Fatal(err)
			}
			a, _ = reloaded.GetByID(a.ID)
			if a.Metadata[tmxRefusalMetadataKey] != nil || tmxTerminalRefusal(a.LastError) {
				t.Fatal("explicit re-enable retained terminal marker")
			}
		}
	}
}

const tmxGoogleValidationBody = `{"error":{"code":403,"status":"PERMISSION_DENIED","message":"Verify at https://example.invalid/verification/session-private?token=synthetic-secret","details":[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"VALIDATION_REQUIRED","metadata":{"verification_url":"https://example.invalid/verification/session-private?token=synthetic-secret"}}]}}`

func TestTMXGoogleValidationStateHasNoResponseData(t *testing.T) {
	originalHooks := log.StandardLogger().ReplaceHooks(make(log.LevelHooks))
	t.Cleanup(func() { log.StandardLogger().ReplaceHooks(originalHooks) })
	hook := logtest.NewGlobal()
	ctx := context.Background()
	for _, provider := range []string{"antigravity"} {
		for _, model := range []string{"", "model"} {
			t.Run(provider+"/"+model, func(t *testing.T) {
				hook.Reset()
				store := &reloadAuthStore{records: make(map[string]*Auth), listEntered: make(chan struct{})}
				h := &recordingHook{}
				m := NewManager(store, nil, h)
				o := &recordingCooldownObserver{}
				SetCooldownObserver(o)
				t.Cleanup(func() { SetCooldownObserver(nil) })
				for _, id := range []string{"refused", "sibling"} {
					if _, err := m.Register(ctx, &Auth{ID: id, Provider: provider, Status: StatusActive}); err != nil {
						t.Fatal(err)
					}
				}
				retry := time.Hour
				failure := Result{AuthID: "refused", Provider: provider, Model: model, RetryAfter: &retry, Error: &Error{HTTPStatus: 403, Retryable: true, Message: tmxGoogleValidationBody, Metadata: map[string]string{"tos_appeal_url": "https://example.invalid/session-private", "verification_url": "synthetic-secret"}}}
				m.MarkResult(ctx, failure)
				got, _ := m.GetByID("refused")
				if !got.Disabled || got.LastError == nil || got.LastError.Code != refusals.GoogleValidation || got.LastError.Message != refusals.StatusMessage(refusals.GoogleValidation) || got.LastError.Retryable || len(got.LastError.Metadata) != 0 {
					t.Fatalf("unsafe or missing validation state: %+v", got)
				}
				if len(got.Metadata) != 2 || got.Metadata["disabled"] != true || got.Metadata[tmxRefusalMetadataKey] != refusals.GoogleValidation {
					t.Fatalf("terminal metadata is not a fixed marker: %+v", got.Metadata)
				}
				assertTMXNoValidationData(t, fmt.Sprintf("%+v %+v %+v %+v", got.LastError, got.Metadata, got.ModelStates[model], o.failures))
				if len(o.failures) != 1 || o.failures[0].Message != refusals.StatusMessage(refusals.GoogleValidation) || o.failures[0].RetryAfter != nil {
					t.Fatalf("unsafe refusal event: %+v", o.failures)
				}
				observed := h.lastResult.Load()
				if observed == nil || observed.Error.Message != refusals.StatusMessage(refusals.GoogleValidation) || len(observed.Error.Metadata) != 0 {
					t.Fatalf("unsafe result hook: %+v", observed)
				}
				if failure.Error.Message != tmxGoogleValidationBody || failure.Error.Metadata["verification_url"] != "synthetic-secret" {
					t.Fatal("classification mutated caller-owned response")
				}
				m.MarkResult(ctx, failure)
				if len(o.failures) != 1 {
					t.Fatal("duplicate refusal event")
				}
				logs := 0
				for _, entry := range hook.AllEntries() {
					assertTMXNoValidationData(t, fmt.Sprintf("%s %+v", entry.Message, entry.Data))
					if strings.HasPrefix(entry.Message, "account refusal:") {
						logs++
					}
				}
				if logs != 1 {
					t.Fatalf("refusal logs = %d", logs)
				}
				sibling, _ := m.GetByID("sibling")
				if sibling.Disabled || sibling.Unavailable || sibling.LastError != nil {
					t.Fatal("refusal affected sibling")
				}
				store.mu.Lock()
				saved := store.records["refused"].Clone()
				store.mu.Unlock()
				assertTMXNoValidationData(t, fmt.Sprintf("%+v %+v", saved.LastError, saved.Metadata))
				// Real stores retain disabled metadata, not runtime error/model fields.
				store.mu.Lock()
				store.records["refused"] = &Auth{ID: saved.ID, Provider: provider, Disabled: saved.Disabled, Metadata: saved.Metadata}
				store.mu.Unlock()
				restarted := NewManager(store, nil, nil)
				if err := restarted.Load(ctx); err != nil {
					t.Fatal(err)
				}
				restored, _ := restarted.GetByID("refused")
				if !restored.Disabled || restored.LastError == nil || restored.LastError.Code != refusals.GoogleValidation || restored.StatusMessage != got.StatusMessage {
					t.Fatal("restart lost validation refusal")
				}
				if m.shouldRefresh(restored, time.Now().Add(365*24*time.Hour)) {
					t.Fatal("validation auth wants refresh")
				}
				assertTMXNoValidationData(t, fmt.Sprintf("%+v %+v", restored.LastError, restored.Metadata))
			})
		}
	}
}

func assertTMXNoValidationData(t *testing.T, value string) {
	t.Helper()
	for _, forbidden := range []string{"https://", "example.invalid", "session-private", "synthetic-secret", "verification_url", `"details"`} {
		if strings.Contains(value, forbidden) {
			t.Fatalf("validation response data retained: %s", value)
		}
	}
}

func TestTMXGoogleValidationRejectsStaleRegistration(t *testing.T) {
	ctx := context.Background()
	m := NewManager(nil, nil, nil)
	old, _ := m.Register(ctx, &Auth{ID: "epoch", Provider: "antigravity", Status: StatusActive})
	current, _ := m.Register(ctx, &Auth{ID: old.ID, Provider: old.Provider, Status: StatusActive})
	if current.RegistrationEpoch <= old.RegistrationEpoch {
		t.Fatal("registration epoch did not advance")
	}
	m.MarkResult(ctx, Result{AuthID: old.ID, Provider: old.Provider, RegistrationEpoch: old.RegistrationEpoch, Error: &Error{HTTPStatus: 403, Message: tmxGoogleValidationBody}})
	got, _ := m.GetByID(old.ID)
	if got.Disabled || got.Status != StatusActive || got.LastError != nil || got.Metadata[tmxRefusalMetadataKey] != nil {
		t.Fatal("stale refusal disabled newer registration")
	}
}

func TestTMXGoogleOtherFailuresRemainNonterminal(t *testing.T) {
	for _, tc := range []struct {
		name, provider, body string
		status               int
	}{
		{"unauthorized", "antigravity", tmxGoogleValidationBody, 401},
		{"rate limited", "antigravity", tmxGoogleValidationBody, 429},
		{"server error", "antigravity", tmxGoogleValidationBody, 500},
		{"missing HTTP status", "antigravity", tmxGoogleValidationBody, 0},
		{"plain permission", "antigravity", `{"error":{"code":403,"status":"PERMISSION_DENIED"}}`, 403},
		{"unproven reason", "antigravity", strings.ReplaceAll(tmxGoogleValidationBody, "VALIDATION_REQUIRED", "OTHER_REASON"), 403},
		{"malformed", "antigravity", tmxGoogleValidationBody[:len(tmxGoogleValidationBody)-1], 403},
		{"non Google", "meta", tmxGoogleValidationBody, 403},
		{"direct gemini", "gemini", tmxGoogleValidationBody, 403},
		{"plugin gemini CLI", "gemini-cli", tmxGoogleValidationBody, 403},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			m := NewManager(nil, nil, nil)
			_, _ = m.Register(ctx, &Auth{ID: "ordinary", Provider: tc.provider, Status: StatusActive})
			m.MarkResult(ctx, Result{AuthID: "ordinary", Provider: tc.provider, Error: &Error{HTTPStatus: tc.status, Message: tc.body}})
			got, _ := m.GetByID("ordinary")
			if got.Disabled || got.Status == StatusDisabled || got.Metadata[tmxRefusalMetadataKey] != nil {
				t.Fatalf("ordinary failure became terminal: %+v", got)
			}
			if tc.status == 403 && !got.NextRetryAfter.After(time.Now()) {
				t.Fatal("ordinary 403 lost cooldown")
			}
			if tc.status == 429 && !got.Quota.Exceeded {
				t.Fatal("ordinary 429 lost quota cooldown")
			}
		})
	}
}

func TestTMXTerminalRefusalPrecedesResultPolicy(t *testing.T) {
	for _, tc := range []struct{ name, code, body, want string }{
		{"validation body", "", tmxGoogleValidationBody, refusals.GoogleValidation},
		{"validation code", refusals.GoogleValidation, refusals.StatusMessage(refusals.GoogleValidation), refusals.GoogleValidation},
		{"tos body", "", strings.ReplaceAll(tmxGoogleValidationBody, "VALIDATION_REQUIRED", "TOS_VIOLATION"), "tos_violation"},
		{"tos code", "tos_violation", "TOS_VIOLATION", "tos_violation"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			m := NewManager(nil, nil, nil)
			_, _ = m.Register(ctx, &Auth{ID: "policy", Provider: "antigravity", Status: StatusActive})
			o := &recordingCooldownObserver{}
			SetCooldownObserver(o)
			t.Cleanup(func() { SetCooldownObserver(nil) })
			calls := 0
			m.SetResultPolicy(ResultPolicyFunc(func(_ context.Context, res Result) Result {
				calls++
				// Mutate the error in place as well as result fields: the original must be independent.
				res.Error.Code = ErrorCodeForceCooldown
				res.Error.Message = "policy retry"
				res.Error.HTTPStatus = 429
				res.CredentialScope = false
				return res
			}))
			for range 2 {
				m.MarkResult(ctx, Result{AuthID: "policy", Provider: "antigravity", Model: "model", Error: &Error{HTTPStatus: 403, Code: tc.code, Message: tc.body}})
			}
			got, _ := m.GetByID("policy")
			if calls != 2 || !got.Disabled || got.LastError == nil || got.LastError.Code != tc.want || got.StatusMessage != refusals.StatusMessage(tc.want) || !got.NextRetryAfter.IsZero() || !got.NextRefreshAfter.IsZero() || len(o.failures) != 1 || o.failures[0].ErrorCode != tc.want {
				t.Fatalf("policy replaced terminal result: auth=%+v events=%+v", got, o.failures)
			}
			if tc.want == refusals.GoogleValidation {
				assertTMXNoValidationData(t, fmt.Sprintf("%+v %+v %+v", got.LastError, got.Metadata, o.failures))
			}
		})
	}
}

func TestTMXOrdinary403RetainsResultPolicy(t *testing.T) {
	ctx := context.Background()
	m := NewManager(nil, nil, nil)
	_, _ = m.Register(ctx, &Auth{ID: "policy", Provider: "antigravity", Status: StatusActive})
	m.SetResultPolicy(ResultPolicyFunc(func(_ context.Context, res Result) Result {
		res.Error = &Error{HTTPStatus: 403, Code: ErrorCodeRequestScoped, Message: "configured stop"}
		return res
	}))
	m.MarkResult(ctx, Result{AuthID: "policy", Provider: "antigravity", Error: &Error{HTTPStatus: 403, Message: "ordinary permission"}})
	got, _ := m.GetByID("policy")
	if got.Disabled || got.Unavailable || !got.NextRetryAfter.IsZero() || got.LastError != nil {
		t.Fatalf("ordinary policy ignored: %+v", got)
	}
}

func TestTMXTerminalRefusalPrecedesRequestScopedAction(t *testing.T) {
	for _, action := range []string{RequestScopedActionStop, RequestScopedActionContinue, RequestScopedActionStopAndCooldown, RequestScopedActionContinueAndCooldown} {
		for _, code := range []string{refusals.GoogleValidation, "tos_violation"} {
			t.Run(action+"/"+code, func(t *testing.T) {
				ctx := context.Background()
				m := NewManager(nil, nil, nil)
				_, _ = m.Register(ctx, &Auth{ID: "scoped", Provider: "antigravity", Status: StatusActive})
				res := Result{AuthID: "scoped", Provider: "antigravity", Error: &Error{HTTPStatus: 403, Code: code, Message: refusals.StatusMessage(code)}}
				applyRequestScopedActionToResult(action, true, &res)
				m.MarkResult(ctx, res)
				got, _ := m.GetByID("scoped")
				if res.Error.Code != code || !got.Disabled || got.LastError.Code != code {
					t.Fatalf("request action replaced terminal code: %+v", res.Error)
				}
				ordinary := Result{Error: &Error{HTTPStatus: 403, Message: "ordinary"}}
				applyRequestScopedActionToResult(action, true, &ordinary)
				want := ErrorCodeRequestScoped
				if action == RequestScopedActionStopAndCooldown || action == RequestScopedActionContinueAndCooldown {
					want = ErrorCodeForceCooldown
				}
				if ordinary.Error.Code != want {
					t.Fatal("ordinary request-scoped behavior changed")
				}
			})
		}
	}
}

func TestTMXQuotaRefusalTransition(t *testing.T) {
	ctx := context.Background()
	for _, code := range []string{refusals.GoogleValidation, "tos_violation"} {
		t.Run(code, func(t *testing.T) {
			store := newTMXRefusalStore()
			hook := &recordingHook{}
			m := NewManager(store, nil, hook)
			observer := &recordingCooldownObserver{}
			SetCooldownObserver(observer)
			defer SetCooldownObserver(nil)
			sent, err := m.Register(ctx, &Auth{ID: "quota", Provider: "antigravity", Status: StatusActive, Metadata: map[string]any{"access_token": "access", "refresh_token": "grant"}, LastError: &Error{Code: "unauthorized", HTTPStatus: 401}, NextRefreshAfter: time.Now().Add(time.Hour)})
			if err != nil {
				t.Fatal(err)
			}
			before, _ := m.GetByID(sent.ID)
			m.refreshLoop = newAuthAutoRefreshLoop(m, time.Minute, 1)
			m.refreshLoop.upsert(sent.ID, time.Now().Add(time.Hour))
			m.refreshLoop.queueReschedule(sent.ID)
			saves := store.saves
			if err = m.TmxRefuseAccount(ctx, sent, code); err != nil {
				t.Fatal(err)
			}
			got, _ := m.GetByID(sent.ID)
			if !got.Disabled || got.Status != StatusDisabled || got.Metadata[tmxRefusalMetadataKey] != code || !got.NextRefreshAfter.IsZero() || got.Generation != before.Generation+1 {
				t.Fatalf("refusal not applied: %+v", got)
			}
			if got.Failed != before.Failed || got.Success != before.Success || got.recentRequests != before.recentRequests || hook.lastResult.Load() != nil {
				t.Fatal("quota refusal recorded an inference")
			}
			if store.saves != saves+1 || len(observer.failures) != 1 {
				t.Fatal("missing save or event")
			}
			if _, scheduled := m.refreshLoop.peek(); scheduled || len(m.refreshLoop.dirty) != 0 {
				t.Fatal("quota refusal left refresh scheduled")
			}
			if err = m.TmxRefuseAccount(ctx, sent, code); err != nil || store.saves != saves+1 || len(observer.failures) != 1 {
				t.Fatal("refusal not idempotent")
			}
			restarted := NewManager(store, nil, nil)
			if err = restarted.Load(ctx); err != nil {
				t.Fatal(err)
			}
			restored, _ := restarted.GetByID(sent.ID)
			if !restored.Disabled || restored.Metadata[tmxRefusalMetadataKey] != code || restarted.shouldRefresh(restored, time.Now().Add(24*time.Hour)) {
				t.Fatal("reload lost durable refusal")
			}
		})
	}
}

func TestTMXQuotaRefusalIdentity(t *testing.T) {
	for _, action := range []string{"reregister", "new-grant", "new-grant-same-access", "same-grant-refresh", "late-after-clear", "non-google", "invalid-code", "delete"} {
		t.Run(action, func(t *testing.T) {
			ctx := context.Background()
			store := newTMXRefusalStore()
			m := NewManager(store, nil, nil)
			observer := &recordingCooldownObserver{}
			SetCooldownObserver(observer)
			defer SetCooldownObserver(nil)
			sent, _ := m.Register(ctx, &Auth{ID: "identity", Provider: "antigravity", Status: StatusActive, Metadata: map[string]any{"access_token": "access", "refresh_token": "grant"}})
			code := refusals.GoogleValidation
			switch action {
			case "reregister":
				m.Remove(ctx, sent.ID)
				_, _ = m.Register(ctx, sent.Clone())
			case "new-grant", "new-grant-same-access", "same-grant-refresh":
				_, _, _ = m.Modify(ctx, sent.ID, sent.RegistrationEpoch, func(a *Auth) error {
					if action != "new-grant-same-access" {
						a.Metadata["access_token"] = "new-access"
					}
					if action == "new-grant" || action == "new-grant-same-access" {
						a.Metadata["refresh_token"] = "new-grant"
					}
					return nil
				})
			case "late-after-clear":
				if err := m.TmxRefuseAccount(ctx, sent, code); err != nil {
					t.Fatal(err)
				}
				_, _, err := m.Modify(WithAccountRefusalClear(ctx), sent.ID, sent.RegistrationEpoch, func(a *Auth) error { a.Disabled = false; a.Status = StatusActive; return nil })
				if err != nil {
					t.Fatal(err)
				}
			case "non-google":
				_, _, _ = m.Modify(ctx, sent.ID, sent.RegistrationEpoch, func(a *Auth) error { a.Provider = "codex"; return nil })
			case "invalid-code":
				code = "not-terminal"
			case "delete":
				m.Remove(ctx, sent.ID)
			}
			saves := store.saves
			events := len(observer.failures)
			before, _ := m.GetByID(sent.ID)
			err := m.TmxRefuseAccount(ctx, sent, code)
			got, exists := m.GetByID(sent.ID)
			apply := action == "same-grant-refresh" || action == "late-after-clear"
			if apply {
				if err != nil || !got.Disabled || store.saves != saves+1 {
					t.Fatalf("same grant lost terminal refusal: %v", err)
				}
			} else {
				if err == nil || exists && got.Disabled || store.saves != saves || len(observer.failures) != events {
					t.Fatalf("stale/invalid refusal applied: %v", err)
				}
			}
			if exists && (got.Failed != before.Failed || got.Success != before.Success || got.recentRequests != before.recentRequests) {
				t.Fatal("quota refusal changed inference accounting")
			}
			if action == "reregister" || action == "new-grant" || action == "new-grant-same-access" || action == "delete" {
				if !errors.Is(err, ErrTmxRefusalStale) {
					t.Fatalf("wrong stale error: %v", err)
				}
			}
		})
	}
}
