package management

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/refusals"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	log "github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
)

type tmxRefreshStore struct {
	mu    sync.Mutex
	saved *coreauth.Auth
	fail  bool
}

func (s *tmxRefreshStore) List(context.Context) ([]*coreauth.Auth, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.saved == nil {
		return nil, nil
	}
	return []*coreauth.Auth{s.saved.Clone()}, nil
}
func (s *tmxRefreshStore) Save(_ context.Context, a *coreauth.Auth) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail {
		return "", errors.New("synthetic-private-store-detail")
	}
	s.saved = a.Clone()
	return "synthetic", nil
}
func (s *tmxRefreshStore) Delete(context.Context, string) error { return nil }

func TestTmxQuotaRefreshRaces(t *testing.T) {
	for _, action := range []string{"metadata", "disable", "clear-refusal", "reregister", "new-grant", "delete"} {
		t.Run(action, func(t *testing.T) {
			ctx := context.Background()
			store := &tmxRefreshStore{}
			m := coreauth.NewManager(store, nil, nil)
			snapshot, err := m.Register(ctx, &coreauth.Auth{ID: "race", Provider: "antigravity", Status: coreauth.StatusActive, Metadata: map[string]any{"access_token": "old", "refresh_token": "grant", "expired": "2000-01-01T00:00:00Z", "note": "old"}})
			if err != nil {
				t.Fatal(err)
			}
			if action == "clear-refusal" {
				snapshot.Metadata["tmx_account_refusal"] = refusals.GoogleValidation
				snapshot.Disabled = true
				snapshot.Status = coreauth.StatusDisabled
				snapshot, err = m.Update(ctx, snapshot)
				if err != nil {
					t.Fatal(err)
				}
			}
			entered, release := make(chan struct{}), make(chan struct{})
			defer func() {
				select {
				case <-release:
				default:
					close(release)
				}
			}()
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				close(entered)
				<-release
				fmt.Fprint(w, `{"access_token":"fresh", "refresh_token":"rotated", "expires_in":3600}`)
			}))
			defer ts.Close()
			oldURL := antigravityOAuthTokenURL
			antigravityOAuthTokenURL = ts.URL
			defer func() { antigravityOAuthTokenURL = oldURL }()
			var usage atomic.Int32
			us := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { usage.Add(1); fmt.Fprint(w, `{}`) }))
			defer us.Close()
			h := &Handler{authManager: m}
			done := make(chan error, 1)
			go func() {
				if action == "clear-refusal" {
					_, err := h.refreshAntigravityOAuthAccessToken(ctx, snapshot, "")
					done <- err
					return
				}
				_, _, err := h.TmxFetch(ctx, snapshot, us.URL, map[string]string{"Authorization": "Bearer $TOKEN$"})
				done <- err
			}()
			<-entered
			switch action {
			case "metadata":
				_, _, err = m.Modify(ctx, snapshot.ID, snapshot.RegistrationEpoch, func(a *coreauth.Auth) error {
					a.Metadata["note"] = "operator"
					a.Metadata["project_id"] = "new-project"
					return nil
				})
			case "disable":
				_, _, err = m.Modify(ctx, snapshot.ID, snapshot.RegistrationEpoch, func(a *coreauth.Auth) error { a.Disabled = true; a.Status = coreauth.StatusDisabled; return nil })
			case "clear-refusal":
				_, _, err = m.Modify(coreauth.WithAccountRefusalClear(ctx), snapshot.ID, snapshot.RegistrationEpoch, func(a *coreauth.Auth) error { a.Disabled = false; a.Status = coreauth.StatusActive; return nil })
			case "reregister":
				m.Remove(ctx, snapshot.ID)
				_, err = m.Register(ctx, &coreauth.Auth{ID: snapshot.ID, Provider: "antigravity", Status: coreauth.StatusActive, Metadata: map[string]any{"access_token": "replacement", "refresh_token": "replacement-grant"}})
			case "new-grant":
				_, _, err = m.Modify(ctx, snapshot.ID, snapshot.RegistrationEpoch, func(a *coreauth.Auth) error {
					a.Metadata["access_token"] = "replacement"
					a.Metadata["refresh_token"] = "replacement-grant"
					return nil
				})
			case "delete":
				m.Remove(ctx, snapshot.ID)
			}
			if err != nil {
				t.Fatal(err)
			}
			close(release)
			err = <-done
			got, exists := m.GetByID(snapshot.ID)
			switch action {
			case "metadata":
				if err != nil || got.Metadata["note"] != "operator" || got.Metadata["project_id"] != "new-project" || got.Metadata["access_token"] != "fresh" || usage.Load() != 1 {
					t.Fatalf("metadata or usage lost: err=%v metadata=%v usage=%d", err, got.Metadata, usage.Load())
				}
			case "disable":
				if err == nil || !got.Disabled || got.Status != coreauth.StatusDisabled || got.Metadata["refresh_token"] != "rotated" || usage.Load() != 0 {
					t.Fatalf("disabled refresh unsafe: err=%v disabled=%v usage=%d", err, got.Disabled, usage.Load())
				}
			case "clear-refusal":
				if err != nil || got.Disabled || got.Metadata["tmx_account_refusal"] != nil || got.LastError != nil || got.Metadata["access_token"] != "fresh" {
					t.Fatalf("clear overwritten: err=%v state=%+v", err, got)
				}
			case "reregister", "new-grant":
				if err == nil || got.Metadata["access_token"] != "replacement" || usage.Load() != 0 {
					t.Fatalf("replacement overwritten/sent: err=%v metadata=%v usage=%d", err, got.Metadata, usage.Load())
				}
			case "delete":
				if err == nil || exists || usage.Load() != 0 {
					t.Fatalf("deleted auth sent/recreated: err=%v exists=%v usage=%d", err, exists, usage.Load())
				}
			}
			if exists {
				saved, _ := store.List(ctx)
				if len(saved) != 1 || fmt.Sprint(saved[0].Metadata) != fmt.Sprint(got.Metadata) || saved[0].Disabled != got.Disabled {
					t.Fatal("saved auth differs from current")
				}
			}
		})
	}
}

func TestTmxQuotaEntryGuard(t *testing.T) {
	for _, mode := range []string{"disabled", "status-disabled", "nil-manager", "current-token", "codex", "claude"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			m := coreauth.NewManager(nil, nil, nil)
			provider := "antigravity"
			if mode == "codex" || mode == "claude" {
				provider = mode
			}
			a, _ := m.Register(ctx, &coreauth.Auth{ID: "entry", Provider: provider, Status: coreauth.StatusActive, Metadata: map[string]any{"access_token": "old", "refresh_token": "grant", "expired": "2000-01-01T00:00:00Z"}})
			var refresh, usage atomic.Int32
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				refresh.Add(1)
				fmt.Fprint(w, `{"access_token":"fresh","expires_in":3600}`)
			}))
			defer ts.Close()
			old := antigravityOAuthTokenURL
			antigravityOAuthTokenURL = ts.URL
			defer func() { antigravityOAuthTokenURL = old }()
			us := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				usage.Add(1)
				if r.Header.Get("Authorization") != "Bearer old" {
					t.Error("non-Google snapshot token changed")
				}
			}))
			defer us.Close()
			h := &Handler{authManager: m}
			switch mode {
			case "disabled", "status-disabled":
				_, _, _ = m.Modify(ctx, a.ID, a.RegistrationEpoch, func(c *coreauth.Auth) error {
					c.Status = coreauth.StatusDisabled
					c.Disabled = mode == "disabled"
					return nil
				})
			case "nil-manager":
				h.authManager = nil
			case "current-token":
				_, _, _ = m.Modify(ctx, a.ID, a.RegistrationEpoch, func(c *coreauth.Auth) error {
					c.Metadata["access_token"] = "current"
					c.Metadata["expired"] = time.Now().Add(time.Hour).Format(time.RFC3339)
					return nil
				})
			case "codex", "claude":
				_, _, _ = m.Modify(ctx, a.ID, a.RegistrationEpoch, func(c *coreauth.Auth) error { c.Metadata["access_token"] = "current"; return nil })
			}
			_, _, err := h.TmxFetch(ctx, a, us.URL, map[string]string{"Authorization": "Bearer $TOKEN$"})
			want := int32(0)
			if mode == "codex" || mode == "claude" {
				want = 1
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil {
				t.Fatal("expected bounded guard error")
			}
			if usage.Load() != want || refresh.Load() != 0 {
				t.Fatalf("usage=%d refresh=%d", usage.Load(), refresh.Load())
			}
		})
	}
}

func TestTmxQuotaRefreshSaveFailure(t *testing.T) {
	ctx := context.Background()
	store := &tmxRefreshStore{}
	m := coreauth.NewManager(store, nil, nil)
	a, _ := m.Register(ctx, &coreauth.Auth{ID: "save", Provider: "antigravity", Status: coreauth.StatusActive, Metadata: map[string]any{"access_token": "old", "refresh_token": "grant", "expired": "2000-01-01T00:00:00Z"}})
	store.fail = true
	hooks := log.StandardLogger().ReplaceHooks(make(log.LevelHooks))
	defer log.StandardLogger().ReplaceHooks(hooks)
	logs := logtest.NewGlobal()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"access_token":"fresh","refresh_token":"rotated","expires_in":3600}`)
	}))
	defer ts.Close()
	oldURL := antigravityOAuthTokenURL
	antigravityOAuthTokenURL = ts.URL
	defer func() { antigravityOAuthTokenURL = oldURL }()
	var usage atomic.Int32
	us := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { usage.Add(1) }))
	defer us.Close()
	h := &Handler{authManager: m}
	_, _, err := h.TmxFetch(ctx, a, us.URL, map[string]string{"Authorization": "Bearer $TOKEN$"})
	got, _ := m.GetByID(a.ID)
	if err == nil || err.Error() != "account changed during request" || got.Metadata["access_token"] != "old" || usage.Load() != 0 {
		t.Fatalf("save failure published/sent: err=%v access=%v usage=%d", err, got.Metadata["access_token"], usage.Load())
	}
	for _, entry := range logs.AllEntries() {
		if strings.Contains(fmt.Sprint(entry), "synthetic-private-store-detail") {
			t.Fatal("store detail leaked")
		}
	}
}

func TestTmxQuotaRefreshWinnerLoser(t *testing.T) {
	ctx := context.Background()
	m := coreauth.NewManager(&tmxRefreshStore{}, nil, nil)
	a, _ := m.Register(ctx, &coreauth.Auth{ID: "two-refreshers", Provider: "antigravity", Status: coreauth.StatusActive, Metadata: map[string]any{"access_token": "old", "refresh_token": "grant", "expired": "2000-01-01T00:00:00Z", "project_id": "original"}})
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		<-release
		fmt.Fprint(w, `{"access_token":"fresh","expires_in":3600}`)
	}))
	defer ts.Close()
	oldURL := antigravityOAuthTokenURL
	antigravityOAuthTokenURL = ts.URL
	defer func() { antigravityOAuthTokenURL = oldURL }()
	var usage atomic.Int32
	us := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer fresh" {
			t.Error("wrong wire token")
		}
		usage.Add(1)
	}))
	defer us.Close()
	h := &Handler{authManager: m}
	type reply struct {
		sent *coreauth.Auth
		err  error
	}
	done := make(chan reply, 2)
	snapshots := []*coreauth.Auth{a.Clone(), a.Clone()}
	for _, snapshot := range snapshots {
		go func(snapshot *coreauth.Auth) {
			_, _, sent, err := h.TmxRequestSent(ctx, snapshot, http.MethodPost, us.URL, map[string]string{"Authorization": "Bearer $TOKEN$"}, []byte(`{"project":"original"}`))
			done <- reply{sent, err}
		}(snapshot)
	}
	<-entered
	<-entered
	close(release)
	winners, losers := 0, 0
	for range snapshots {
		r := <-done
		if r.err == nil {
			winners++
			current, _ := m.GetByID(a.ID)
			if r.sent == nil || r.sent.CredentialVersion != current.CredentialVersion || r.sent.CredentialVersion <= a.CredentialVersion {
				t.Fatal("winner missing committed identity")
			}
		} else {
			losers++
			if !errors.Is(r.err, errTmxAuthChanged) {
				t.Fatalf("loser=%v", r.err)
			}
		}
	}
	if winners != 1 || losers != 1 || usage.Load() != 1 {
		t.Fatalf("winners=%d losers=%d usage=%d", winners, losers, usage.Load())
	}
	for _, snapshot := range snapshots {
		if snapshot.CredentialVersion != a.CredentialVersion || snapshot.Metadata["access_token"] != "old" || snapshot.Metadata["expired"] != a.Metadata["expired"] {
			t.Fatal("flight mutated caller snapshot")
		}
	}
}

func TestTmxQuotaResponseDropsReplacedGrant(t *testing.T) {
	ctx := context.Background()
	m := coreauth.NewManager(nil, nil, nil)
	a, _ := m.Register(ctx, &coreauth.Auth{ID: "during-send", Provider: "antigravity", Status: coreauth.StatusActive, Metadata: map[string]any{"access_token": "old", "refresh_token": "grant", "expired": time.Now().Add(time.Hour).Format(time.RFC3339)}})
	entered, release := make(chan struct{}), make(chan struct{})
	us := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
		w.WriteHeader(403)
		fmt.Fprint(w, `{"error":{"code":403,"status":"PERMISSION_DENIED","details":[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"VALIDATION_REQUIRED"}]}}`)
	}))
	defer us.Close()
	h := &Handler{authManager: m}
	done := make(chan error, 1)
	go func() {
		status, body, sent, err := h.TmxRequestSent(ctx, a, http.MethodGet, us.URL, map[string]string{"Authorization": "Bearer $TOKEN$"}, nil)
		if status != 0 || body != nil || sent == nil || sent.CredentialVersion != a.CredentialVersion {
			t.Error("stale response leaked or sent identity lost")
		}
		if stale := m.TmxRefuseAccount(ctx, sent, refusals.GoogleValidation); !errors.Is(stale, coreauth.ErrTmxRefusalStale) {
			t.Errorf("stale refusal=%v", stale)
		}
		done <- err
	}()
	<-entered
	_, _, err := m.Modify(ctx, a.ID, a.RegistrationEpoch, func(current *coreauth.Auth) error {
		current.Metadata["access_token"] = "replacement"
		current.Metadata["refresh_token"] = "new-grant"
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	close(release)
	if err = <-done; !errors.Is(err, errTmxAuthChanged) {
		t.Fatalf("stale response=%v", err)
	}
	got, _ := m.GetByID(a.ID)
	if got.Disabled || got.Metadata["tmx_account_refusal"] != nil {
		t.Fatal("old reply disabled replacement")
	}
}
