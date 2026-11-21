package quota

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/refusals"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

const agValidationBody = `{"error":{"code":403,"status":"PERMISSION_DENIED","details":[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"VALIDATION_REQUIRED"}]}}`
const agTOSBody = `{"error":{"code":403,"status":"PERMISSION_DENIED","details":[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"TOS_VIOLATION"}]}}`

type agSentFetcher struct {
	*agFetcher
	sent *coreauth.Auth
}

func (f *agSentFetcher) TmxRequestSent(ctx context.Context, a *coreauth.Auth, method, url string, h map[string]string, b []byte) (int, []byte, *coreauth.Auth, error) {
	status, body, err := f.TmxRequest(ctx, a, method, url, h, b)
	return status, body, f.sent, err
}

type agRefusalFetcher struct {
	*agSentFetcher
	refuse  func(*coreauth.Auth, string) error
	refused []*coreauth.Auth
}

func (f *agRefusalFetcher) TmxRefuseAccount(_ context.Context, sent *coreauth.Auth, code string) error {
	f.refused = append(f.refused, sent)
	return f.refuse(sent, code)
}

func agRefusalService(t *testing.T, f Fetcher, now *time.Time) *Service {
	t.Helper()
	db := sql.OpenDB(&agSQLSink{})
	t.Cleanup(func() { _ = db.Close() })
	return &Service{db: db, table: "synthetic.quota_snapshots", fetch: f, now: func() time.Time { return *now }, pollEvery: 10 * time.Minute, slowEvery: time.Hour, accounts: map[string]*account{}}
}
func agQuotaAuth(id string) *coreauth.Auth {
	return &coreauth.Auth{ID: id, Provider: "antigravity", Status: coreauth.StatusActive, RegistrationEpoch: 1, CredentialVersion: 1, Metadata: map[string]any{"project_id": "synthetic"}}
}

func TestAntigravityQuotaTerminalAndOrdinary403(t *testing.T) {
	for _, tc := range []struct {
		name, body, code string
		status           int
	}{
		{"validation", agValidationBody, refusals.GoogleValidation, 403}, {"tos", agTOSBody, "tos_violation", 403},
		{"generic", `{"error":{"code":403,"status":"PERMISSION_DENIED"}}`, "", 403},
		{"non-json", "VALIDATION_REQUIRED", "", 403}, {"message-only", `{"error":{"code":403,"status":"PERMISSION_DENIED","message":"VALIDATION_REQUIRED"}}`, "", 403},
		{"unauthorized", agValidationBody, "", 401},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)
			a := agQuotaAuth("classify-" + tc.name)
			base := &agFetcher{auths: []*coreauth.Auth{a}, replies: map[string]fakeRoute{a.ID: {tc.status, tc.body}}}
			f := &agRefusalFetcher{agSentFetcher: &agSentFetcher{agFetcher: base, sent: a.Clone()}}
			f.refuse = func(sent *coreauth.Auth, code string) error {
				if code != tc.code {
					t.Errorf("code=%q", code)
				}
				a.Disabled = true
				a.Status = coreauth.StatusDisabled
				return nil
			}
			s := agRefusalService(t, f, &now)
			s.tick(context.Background())
			acc := s.accounts[a.ID]
			if tc.code != "" {
				if len(f.refused) != 1 || !acc.pauseUntil.IsZero() || acc.PollError != refusals.StatusMessage(tc.code) || acc.refused != nil {
					t.Fatal("terminal refusal did not use durable path")
				}
				now = now.Add(24 * time.Hour)
				s.tick(context.Background())
				if len(base.requests) != 1 {
					t.Fatal("disabled account polled")
				}
				a.Disabled = false
				a.Status = coreauth.StatusActive
				base.replies[a.ID] = fakeRoute{200, agFixture("0.5", "2026-11-23T00:00:00Z")}
				s.tick(context.Background())
				if len(base.requests) != 2 || acc.PollError != "" {
					t.Fatal("operator re-enable did not resume polling")
				}
			} else {
				if len(f.refused) != 0 || !acc.pauseUntil.Equal(now.Add(authErrorPause)) || a.Disabled {
					t.Fatal("ordinary error became terminal or lost backoff")
				}
			}
		})
	}
}

func TestAntigravityQuotaLocalStopAndSentIdentity(t *testing.T) {
	for _, mode := range []string{"missing-refuser", "nil-sent", "persist-error", "stale", "sent-identity"} {
		t.Run(mode, func(t *testing.T) {
			now := time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)
			a, b := agQuotaAuth("stop-"+mode), agQuotaAuth("healthy-"+mode)
			sent := a.Clone()
			sent.CredentialVersion = 2
			base := &agFetcher{auths: []*coreauth.Auth{a, b}, replies: map[string]fakeRoute{a.ID: {403, agValidationBody}, b.ID: {200, agFixture("0.75", "2026-11-24T00:00:00Z")}}}
			sf := &agSentFetcher{agFetcher: base, sent: sent}
			rf := &agRefusalFetcher{agSentFetcher: sf}
			rf.refuse = func(got *coreauth.Auth, code string) error {
				if got != sent {
					t.Error("refusal used waiter identity")
				}
				switch mode {
				case "stale":
					return coreauth.ErrTmxRefusalStale
				case "persist-error":
					return errors.New("private-synthetic-detail")
				}
				a.Disabled = true
				a.Status = coreauth.StatusDisabled
				return nil
			}
			var fetch Fetcher = rf
			if mode == "missing-refuser" {
				fetch = sf
			}
			if mode == "nil-sent" {
				sf.sent = nil
			}
			s := agRefusalService(t, fetch, &now)
			s.tick(context.Background())
			acc := s.accounts[a.ID]
			if !acc.pauseUntil.IsZero() || strings.Contains(acc.PollError, "private") {
				t.Fatal("terminal refusal got hourly pause/private detail")
			}
			stop := mode == "missing-refuser" || mode == "nil-sent" || mode == "persist-error"
			if stop {
				if sf.sent != nil {
					a.CredentialVersion = sent.CredentialVersion
				}
				if acc.refused == nil {
					t.Fatal("missing keyed local stop")
				}
				stoppedAt := now
				for _, delta := range []time.Duration{2 * time.Hour, 24 * time.Hour} {
					now = stoppedAt.Add(delta)
					s.tick(context.Background())
					_, throttled, err := s.PollNow(context.Background(), a.ID)
					if err != nil || !throttled {
						t.Fatal("local stop lost")
					}
				}
				aCount, bCount := 0, 0
				for _, r := range base.requests {
					if r.id == a.ID {
						aCount++
					}
					if r.id == b.ID {
						bCount++
					}
				}
				if aCount != 1 || bCount != 3 {
					t.Fatalf("stop crossed account boundary: %d %d", aCount, bCount)
				}
				a.CredentialVersion++
				a.Metadata["access_token"] = "replacement-access"
				a.Metadata["refresh_token"] = "replacement-grant"
				base.replies[a.ID] = fakeRoute{200, agFixture("0.25", "2026-11-25T00:00:00Z")}
				now = now.Add(11 * time.Minute)
				s.tick(context.Background())
				if acc.refused == nil {
					t.Fatal("same-epoch replacement cleared fallback stop")
				}
				a.Disabled, a.Status = true, coreauth.StatusDisabled
				s.tick(context.Background())
				a.Disabled, a.Status = false, coreauth.StatusActive
				s.tick(context.Background())
				if acc.refused == nil {
					t.Fatal("disable/re-enable cleared fallback stop")
				}
				a.RegistrationEpoch++
				s.tick(context.Background())
				if acc.refused != nil || acc.PollError != "" {
					t.Fatal("re-registration did not clear local stop")
				}
			} else if mode == "stale" {
				if acc.refused != nil {
					t.Fatal("stale refusal stopped account")
				}
				now = now.Add(11 * time.Minute)
				s.tick(context.Background())
				if len(rf.refused) != 2 {
					t.Fatal("stale refusal did not resume normal cadence")
				}
			}
			if mode == "nil-sent" && len(rf.refused) != 0 {
				t.Fatal("nil sent reached refuser")
			}
		})
	}
}

// This fetcher synchronizes replacement and an abandoned shared read without
// touching the caller snapshot from the provider goroutine.
type agCanceledFetcher struct {
	mu                         sync.Mutex
	auth                       *coreauth.Auth
	entered, release, finished chan struct{}
	calls                      int
	refused                    int
}

func (f *agCanceledFetcher) TmxAuths() []*coreauth.Auth {
	f.mu.Lock()
	defer f.mu.Unlock()
	return []*coreauth.Auth{f.auth.Clone()}
}
func (f *agCanceledFetcher) TmxFetch(context.Context, *coreauth.Auth, string, map[string]string) (int, []byte, error) {
	panic("unexpected GET")
}
func (f *agCanceledFetcher) TmxRequest(ctx context.Context, a *coreauth.Auth, m, u string, h map[string]string, b []byte) (int, []byte, error) {
	s, data, _, err := f.TmxRequestSent(ctx, a, m, u, h, b)
	return s, data, err
}
func (f *agCanceledFetcher) TmxRequestSent(_ context.Context, a *coreauth.Auth, _, _ string, _ map[string]string, _ []byte) (int, []byte, *coreauth.Auth, error) {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	if a.CredentialVersion == 1 {
		close(f.entered)
		<-f.release
		defer close(f.finished)
		return 403, []byte(agValidationBody), a.Clone(), nil
	}
	return 200, []byte(agFixture("0.5", "2026-11-24T00:00:00Z")), a.Clone(), nil
}
func (f *agCanceledFetcher) TmxRefuseAccount(context.Context, *coreauth.Auth, string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.refused++
	return nil
}

func TestAntigravityQuotaCancelledLeaderReplacedGrant(t *testing.T) {
	now := time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)
	f := &agCanceledFetcher{auth: agQuotaAuth("cancel-replace"), entered: make(chan struct{}), release: make(chan struct{}), finished: make(chan struct{})}
	s := agRefusalService(t, f, &now)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _, _, _ = s.PollNow(ctx, f.auth.ID) }()
	<-f.entered
	cancel()
	<-done
	f.mu.Lock()
	replacement := f.auth.Clone()
	replacement.CredentialVersion = 2
	f.auth = replacement
	f.mu.Unlock()
	now = now.Add(time.Minute)
	acc, throttled, err := s.PollNow(context.Background(), replacement.ID)
	if err != nil || throttled || len(acc.Windows) != 1 || *acc.Windows[0].UsedPct != 50 {
		t.Fatalf("replacement joined canceled old read: %+v %v", acc, err)
	}
	close(f.release)
	<-f.finished
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.calls != 2 || f.refused != 0 || !s.accounts[replacement.ID].pauseUntil.IsZero() {
		t.Fatal("abandoned reply affected replacement")
	}
}

type nonGoogleRefusalFetcher struct {
	*fakeFetcher
	refusals int
}

func (f *nonGoogleRefusalFetcher) TmxRefuseAccount(context.Context, *coreauth.Auth, string) error {
	f.refusals++
	return nil
}
func TestQuotaGoogle403DoesNotRefuseClaude(t *testing.T) {
	now := time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)
	a := &coreauth.Auth{ID: "claude-google-body", Provider: "claude"}
	f := &nonGoogleRefusalFetcher{fakeFetcher: &fakeFetcher{auths: []*coreauth.Auth{a}, status: 403, usage: agValidationBody, calls: map[string]int{}}}
	s := agRefusalService(t, f, &now)
	s.tick(context.Background())
	if f.refusals != 0 || !s.accounts[a.ID].pauseUntil.Equal(now.Add(authErrorPause)) {
		t.Fatal("Google body changed non-Google handling")
	}
}

func TestAntigravityQuotaReenableKeepsCadence(t *testing.T) {
	now := time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)
	a := agQuotaAuth("reenable-cadence")
	base := &agFetcher{auths: []*coreauth.Auth{a}, replies: map[string]fakeRoute{a.ID: {403, agValidationBody}}}
	f := &agRefusalFetcher{agSentFetcher: &agSentFetcher{agFetcher: base, sent: a.Clone()}}
	f.refuse = func(*coreauth.Auth, string) error { a.Disabled = true; a.Status = coreauth.StatusDisabled; return nil }
	s := agRefusalService(t, f, &now)
	s.tick(context.Background())
	a.Disabled = false
	a.Status = coreauth.StatusActive
	base.replies[a.ID] = fakeRoute{200, agFixture("0.25", "2026-11-22T00:00:00Z")}
	now = now.Add(9 * time.Minute)
	s.tick(context.Background())
	if len(base.requests) != 1 {
		t.Fatal("re-enable bypassed ten minute cadence")
	}
	now = now.Add(2 * time.Minute)
	s.tick(context.Background())
	if len(base.requests) != 2 || s.accounts[a.ID].PollError != "" || len(s.accounts[a.ID].current) != 1 {
		t.Fatal("re-enable did not resume ten minute cadence")
	}
}

// A refresh can move v1 to v2 inside the flight. The fallback stop must use
// sent v2, otherwise the next tick's current v2 immediately clears it.
func TestAntigravityQuotaLocalStopUsesRefreshedSent(t *testing.T) {
	for _, mode := range []string{"missing-refuser", "persist-error"} {
		t.Run(mode, func(t *testing.T) {
			now := time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)
			waiter, b := agQuotaAuth("refreshed-stop-"+mode), agQuotaAuth("refreshed-healthy-"+mode)
			waiter.Metadata["access_token"], waiter.Metadata["refresh_token"] = "old-access", "same-grant"
			sent := waiter.Clone()
			sent.CredentialVersion = 2
			base := &agFetcher{auths: []*coreauth.Auth{waiter, b}, replies: map[string]fakeRoute{waiter.ID: {403, agValidationBody}, b.ID: {200, agFixture("0.5", "2026-11-24T00:00:00Z")}}}
			sf := &agSentFetcher{agFetcher: base, sent: sent}
			var fetch Fetcher = sf
			if mode == "persist-error" {
				fetch = &agRefusalFetcher{agSentFetcher: sf, refuse: func(*coreauth.Auth, string) error { return errors.New("synthetic save failed") }}
			}
			s := agRefusalService(t, fetch, &now)
			s.tick(context.Background())
			if stop := s.accounts[waiter.ID].refused; stop == nil || stop.epoch != sent.RegistrationEpoch || stop.version != sent.CredentialVersion {
				t.Fatal("fallback stop lost actual sent attribution")
			}
			base.auths[0] = sent.Clone()
			base.auths[0].CredentialVersion++
			base.auths[0].Metadata["access_token"] = "same-grant-refreshed"
			stoppedAt := now
			for _, delta := range []time.Duration{2 * time.Hour, 24 * time.Hour} {
				now = stoppedAt.Add(delta)
				s.tick(context.Background())
				_, throttled, err := s.PollNow(context.Background(), sent.ID)
				if err != nil || !throttled {
					t.Fatal("refreshed sent identity did not stop polling")
				}
			}
			aCount, bCount := 0, 0
			for _, req := range base.requests {
				if req.id == sent.ID {
					aCount++
				} else if req.id == b.ID {
					bCount++
				}
			}
			if aCount != 1 || bCount != 3 {
				t.Fatalf("sent stop lost/account isolation broken: %d %d", aCount, bCount)
			}
		})
	}
}
