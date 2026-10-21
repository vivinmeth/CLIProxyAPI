package resets

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

func pctw(name string, used float64) Window { return Window{Window: name, UsedPct: &used} }

func TestRecoveryOf(t *testing.T) {
	for _, tc := range []struct {
		name       string
		provider   string
		before     []Window
		after      []Window
		afterRead  bool
		clears     []string
		windows    []string
		credential bool
		model      bool
	}{
		{name: "full claude reset", provider: "claude",
			before: []Window{pctw("5h", 100), pctw("7d", 100)}, after: []Window{pctw("5h", 0), pctw("7d", 0)}, afterRead: true,
			clears: []string{"5h", "7d", "7d_oi"}, windows: []string{"5h", "7d", "7d_oi"}, credential: true, model: true},
		{name: "5h grant while the week is exhausted", provider: "claude",
			before: []Window{pctw("5h", 100), pctw("7d", 100)}, after: []Window{pctw("5h", 0), pctw("7d", 100)}, afterRead: true,
			clears: []string{"5h"}, windows: []string{"5h"}},
		{name: "reading lags: nothing verified", provider: "claude",
			before: []Window{pctw("5h", 100), pctw("7d", 40)}, after: []Window{pctw("5h", 100), pctw("7d", 40)}, afterRead: true,
			clears: []string{"5h", "7d", "7d_oi"}},
		{name: "unreadable 7d_oi needs every readable window recovered", provider: "claude",
			before: []Window{pctw("5h", 30), pctw("7d", 100)}, after: []Window{pctw("5h", 0), pctw("7d", 100)}, afterRead: true,
			clears: []string{"5h", "7d", "7d_oi"}, windows: []string{"5h"}},
		{name: "unknown scope (legacy claim) is every window", provider: "claude",
			before: []Window{pctw("5h", 100), pctw("7d", 60)}, after: []Window{pctw("5h", 0), pctw("7d", 0)}, afterRead: true,
			windows: []string{"5h", "7d", "7d_oi"}, credential: true, model: true},
		{name: "codex reset", provider: "codex",
			before: []Window{pctw("5h", 100), pctw("7d", 70)}, after: []Window{pctw("5h", 0), pctw("7d", 0)}, afterRead: true,
			clears: []string{"5h", "7d"}, windows: []string{"5h", "7d"}, credential: true},
		{name: "no post-read: scope covers what was exhausted", provider: "claude",
			before: []Window{pctw("5h", 100), pctw("7d", 50)}, clears: []string{"5h"}, credential: true},
		{name: "no post-read: an exhausted window outside the scope keeps the cooldown", provider: "claude",
			before: []Window{pctw("5h", 100), pctw("7d", 100)}, clears: []string{"5h"}},
		{name: "no post-read: Fable window in scope", provider: "claude",
			before: []Window{pctw("5h", 20), pctw("7d", 30)}, clears: []string{"5h", "7d", "7d_oi"}, credential: true, model: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := recoveryOf(tc.provider, tc.before, tc.after, tc.afterRead, tc.clears)
			slices.Sort(got.Windows)
			want := append([]string(nil), tc.windows...)
			slices.Sort(want)
			if !slices.Equal(got.Windows, want) || got.CredentialQuota != tc.credential || slices.Equal(got.ModelWindows, []string{"7d_oi"}) != tc.model {
				t.Fatalf("recovery = %+v; want windows %v credential %v model %v", got, want, tc.credential, tc.model)
			}
		})
	}
}

// claudeGrantBody is a usage reading with one usable grant clearing the given windows.
func claudeGrantBody(fiveHour, sevenDay float64, clears ...string) string {
	return fmt.Sprintf(`{"five_hour": {"utilization": %v, "resets_at": "2026-10-20T18:20:00+00:00"},
 "seven_day": {"utilization": %v, "resets_at": "2026-10-27T08:00:00+00:00"},
 "cedar_ember": {"eligible": true, "at_limit": true, "grants": [{"id": "g1", "label": "Reset", "resets_left": 1,
  "ends_at": "2026-12-01T00:00:00Z", "clears": ["%s"], "paused": false, "usable_now": true}]}}`,
		fiveHour, sevenDay, strings.Join(clears, `","`))
}

// The red-team scenario (multi-account review, finding 7): a 5h-only grant while the weekly
// window is exhausted must not reopen the account, and must never clear an unrelated block.
func TestRedeemReleasesOnlyRecoveredWindows(t *testing.T) {
	coreauth.SetQuotaCooldownDisabled(false)
	manager := coreauth.NewManager(nil, nil, nil)
	auth := claudeAuth()
	auth.Quota.Signals = map[string]string{
		"Anthropic-Ratelimit-Unified-5h-Utilization": "1.0",
		"Anthropic-Ratelimit-Unified-7d-Utilization": "1.0",
		"Anthropic-Ratelimit-Unified-7d-Reset":       "1793500000",
	}
	auth.Quota.ObservedAt = time.Now()
	if _, err := manager.Register(coreauth.WithSkipPersist(context.Background()), auth); err != nil {
		t.Fatal(err)
	}
	transient := 10 * time.Minute
	manager.MarkResult(context.Background(), coreauth.Result{AuthID: auth.ID, Provider: "claude", Model: "claude-sonnet-5",
		RetryAfter: &transient, Error: &coreauth.Error{HTTPStatus: http.StatusServiceUnavailable, Message: "unavailable"}})
	weekly := 72 * time.Hour
	manager.MarkResult(context.Background(), coreauth.Result{AuthID: auth.ID, Provider: "claude", Model: "claude-opus-5",
		CredentialScope: true, RetryAfter: &weekly, Error: &coreauth.Error{HTTPStatus: http.StatusTooManyRequests, Message: "weekly limit"}})

	redeemed := false
	f := &fakeBackend{auths: []*coreauth.Auth{auth}}
	f.handle = func(method, url string, _ []byte) (int, string, error) {
		switch {
		case strings.Contains(url, "/reset_rate_limits"):
			redeemed = true
			return 200, `{"result": "reset"}`, nil
		case redeemed:
			return 200, claudeGrantBody(0, 100, "five_hour"), nil
		}
		return 200, claudeGrantBody(100, 100, "five_hour"), nil
	}
	f.clear = func(id string, r coreauth.QuotaRecovery) error {
		_, err := manager.ClearRecoveredQuota(context.Background(), id, r)
		return err
	}
	res, err := newTestService(f).Redeem(context.Background(), auth.ID, "g1", "test")
	if err != nil {
		t.Fatal(err)
	}
	if r := res.Redemption; r.Outcome != OutcomeReset || !r.Confirmed || strings.Join(r.Clears, ",") != "5h" {
		t.Fatalf("redemption %+v", r)
	}
	// The release came after the verifying read.
	lastRead, clearAt := -1, -1
	for i, c := range f.calls {
		if strings.HasPrefix(c, "GET "+claudeStatusURL) {
			lastRead = i
		}
		if strings.HasPrefix(c, "CLEAR ") {
			clearAt = i
		}
	}
	if clearAt < 0 || clearAt < lastRead {
		t.Fatalf("the release must follow the post-redeem read: %v", f.calls)
	}

	updated, _ := manager.GetByID(auth.ID)
	if updated.Quota.Reason != "credential_quota" || !updated.ModelStates["claude-opus-5"].Unavailable {
		t.Fatalf("the weekly limit is still exhausted; the account must stay cooled: %+v", updated.Quota)
	}
	if sonnet := updated.ModelStates["claude-sonnet-5"]; !sonnet.Unavailable {
		t.Fatal("an unrelated transient block must survive a redeem")
	}
	if _, ok := updated.Quota.Signals["Anthropic-Ratelimit-Unified-5h-Utilization"]; ok {
		t.Fatalf("the recovered 5h signals must go: %v", updated.Quota.Signals)
	}
	if _, ok := updated.Quota.Signals["Anthropic-Ratelimit-Unified-7d-Reset"]; !ok {
		t.Fatalf("the weekly signals must stay: %v", updated.Quota.Signals)
	}
}

func TestRedeemWithoutPostReadReleasesOnlyScopedCooldown(t *testing.T) {
	redeemed := false
	f := &fakeBackend{auths: []*coreauth.Auth{claudeAuth()}}
	f.handle = func(method, url string, _ []byte) (int, string, error) {
		switch {
		case strings.Contains(url, "/reset_rate_limits"):
			redeemed = true
			return 200, `{"result": "reset"}`, nil
		case redeemed:
			return 503, "", nil
		}
		return 200, claudeGrantBody(100, 100, "five_hour"), nil
	}
	res, err := newTestService(f).Redeem(context.Background(), "claude-1", "g1", "test")
	if err != nil {
		t.Fatal(err)
	}
	if res.Redemption.Confirmed || res.Redemption.Warning != "unconfirmed" {
		t.Fatalf("redemption %+v", res.Redemption)
	}
	if len(f.recoveries) != 1 {
		t.Fatalf("recoveries %+v", f.recoveries)
	}
	if r := f.recoveries[0]; r.CredentialQuota || len(r.ModelWindows) != 0 || len(r.Windows) != 0 {
		t.Fatalf("an unverified 5h grant with the week exhausted releases nothing: %+v", r)
	}
}

func TestRedeemRetryUsesJournaledScope(t *testing.T) {
	answer := `{"result": "unavailable"}`
	redeemed := false
	f := &fakeBackend{auths: []*coreauth.Auth{claudeAuth()}}
	f.handle = func(method, url string, _ []byte) (int, string, error) {
		switch {
		case strings.Contains(url, "/reset_rate_limits"):
			if answer != `{"result": "unavailable"}` {
				redeemed = true
			}
			return 200, answer, nil
		case redeemed:
			// The grant is gone from the list once used.
			return 200, `{"five_hour": {"utilization": 0}, "seven_day": {"utilization": 100}}`, nil
		}
		return 200, claudeGrantBody(100, 100, "five_hour"), nil
	}
	s := newTestService(f)
	res, err := s.Redeem(context.Background(), "claude-1", "g1", "test")
	if err != nil || res.Redemption.State != "pending" || strings.Join(res.Redemption.Clears, ",") != "5h" {
		t.Fatalf("first claim must be pending with its scope journaled: %+v %v", res, err)
	}
	answer = `{"result": "already_used"}`
	if res, err = s.Redeem(context.Background(), "claude-1", "g1", "test"); err != nil {
		t.Fatal(err)
	}
	if res.Redemption.Outcome != OutcomeAlreadyRedeemed || len(f.recoveries) != 1 {
		t.Fatalf("retry: %+v, recoveries %+v", res.Redemption, f.recoveries)
	}
	if r := f.recoveries[0]; r.CredentialQuota || strings.Join(r.Windows, ",") != "5h" {
		t.Fatalf("the retry must release only the journaled 5h scope: %+v", r)
	}
}
