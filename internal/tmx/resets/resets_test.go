package resets

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

var testNow = time.Date(2026, 10, 20, 17, 0, 0, 0, time.UTC)

const claudeBody = `{
 "five_hour": {"utilization": 9.0, "resets_at": "2026-10-20T18:20:00+00:00"},
 "seven_day": {"utilization": 6.0, "resets_at": "2026-10-27T08:00:00+00:00"},
 "seven_day_opus": null,
 "cedar_ember": {"eligible": true, "ineligible_reason": null, "at_limit": false,
  "grants": [{"id": "launch_reset", "label": "Launch reset\u0007", "resets_total": 1, "resets_left": 1,
    "starts_at": "2026-09-22T00:00:00Z", "ends_at": "2026-10-22T16:30:00Z",
    "clears": ["five_hour", "seven_day", "seven_day_overage_included", "mystery"],
    "paused": false, "usable_now": true, "use_requires_limit": false, "blocking": []},
   {"id": "later_one", "label": "Later", "resets_left": 1, "ends_at": "2026-12-01T00:00:00Z",
    "clears": ["five_hour"], "paused": false, "usable_now": true, "use_requires_limit": true}],
  "next_grant_id": "launch_reset", "weekly_resets_at": "2026-10-27T08:00:00Z", "cooldown_until": null}
}`

const codexUsageBody = `{"plan_type": "prolite", "rate_limit": {"allowed": true, "limit_reached": false,
 "primary_window": {"used_percent": 0, "limit_window_seconds": 604800, "reset_after_seconds": 586900, "reset_at": 1793102375},
 "secondary_window": null}, "rate_limit_reset_credits": {"available_count": 3, "applicable_available_count": 0}}`

const codexCreditsBody = `{"credits": [
 {"id": "c3", "reset_type": "codex_rate_limits", "is_supported_by_plan": true, "status": "available", "granted_at": "2026-10-08T00:00:00Z", "expires_at": "2026-11-07T00:00:00Z", "title": "Full reset"},
 {"id": "c1", "reset_type": "codex_rate_limits", "is_supported_by_plan": true, "status": "available", "granted_at": "2026-09-22T00:00:00Z", "expires_at": "2026-10-22T20:24:00Z", "title": "Full reset"},
 {"id": "used", "reset_type": "codex_rate_limits", "status": "redeemed", "expires_at": "2026-11-01T00:00:00Z"},
 {"id": "other", "reset_type": "something_else", "status": "available", "expires_at": "2026-11-01T00:00:00Z"}
], "available_count": 3}`

func TestParseClaude(t *testing.T) {
	st, err := ParseClaude([]byte(claudeBody), testNow)
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Windows) != 2 || st.Windows[0].Window != "5h" || *st.Windows[0].UsedPct != 9 || st.Windows[1].Window != "7d" {
		t.Fatalf("windows %+v", st.Windows)
	}
	if !st.Eligible || st.NextCreditID != "launch_reset" || len(st.Credits) != 2 {
		t.Fatalf("status %+v", st)
	}
	c := st.Credits[0]
	if c.ID != "launch_reset" || c.Title != "Launch reset" || c.Blocker != "" || strings.Join(c.Clears, ",") != "5h,7d,7d_oi" {
		t.Fatalf("first credit %+v", c)
	}
	if st.Credits[1].Blocker != "not_limited" {
		t.Fatalf("use_requires_limit without at_limit must block: %+v", st.Credits[1])
	}
}

func TestParseClaudeBlockers(t *testing.T) {
	var root map[string]any
	_ = json.Unmarshal([]byte(claudeBody), &root)
	ce := root["cedar_ember"].(map[string]any)
	ce["cooldown_until"] = "2026-10-20T18:00:00Z"
	b, _ := json.Marshal(root)
	st, _ := ParseClaude(b, testNow)
	if st.Credits[0].Blocker != "cooldown" {
		t.Fatalf("cooldown: %+v", st.Credits[0])
	}
	ce["cooldown_until"] = nil
	ce["eligible"] = false
	b, _ = json.Marshal(root)
	st, _ = ParseClaude(b, testNow)
	if st.Credits[0].Blocker != "ineligible" {
		t.Fatalf("ineligible: %+v", st.Credits[0])
	}
	st, _ = ParseClaude([]byte(claudeBody), time.Date(2026, 10, 23, 0, 0, 0, 0, time.UTC))
	if st.Credits[0].Blocker != "expired" {
		t.Fatalf("expired: %+v", st.Credits[0])
	}
	if _, err := ParseClaude([]byte(`not json`), testNow); err == nil {
		t.Fatal("malformed body must fail")
	}
	st, err := ParseClaude([]byte(`{"five_hour": {"utilization": 1}}`), testNow)
	if err != nil || len(st.Credits) != 0 {
		t.Fatalf("no cedar_ember = no credits, no error: %v %+v", err, st)
	}
}

func TestParseCodex(t *testing.T) {
	st, err := ParseCodexUsage([]byte(codexUsageBody), testNow)
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Windows) != 1 || st.Windows[0].Window != "7d" || st.ApplicableCount == nil || *st.ApplicableCount != 0 {
		t.Fatalf("usage %+v", st)
	}
	if err = ParseCodexCredits([]byte(codexCreditsBody), &st, testNow); err != nil {
		t.Fatal(err)
	}
	if len(st.Credits) != 2 || st.Credits[0].ID != "c1" || st.Credits[1].ID != "c3" {
		t.Fatalf("credits must be soonest-expiry first, available only: %+v", st.Credits)
	}
	if st.Credits[0].Warning != "nothing_to_clear" || st.Credits[0].Blocker != "" || st.Credits[0].Clears[0] != "7d" {
		t.Fatalf("credit %+v", st.Credits[0])
	}
	if err = ParseCodexCredits([]byte(`{"nope": 1}`), &st, testNow); err == nil {
		t.Fatal("missing credits[] must fail")
	}
}

// fakeBackend answers provider calls from a script.
type fakeBackend struct {
	mu       sync.Mutex
	auths    []*coreauth.Auth
	handle   func(method, url string, body []byte) (int, string, error)
	calls    []string
	bodies   []string
	cooldown []string
	signals  []string
}

func (f *fakeBackend) TmxAuths() []*coreauth.Auth { return f.auths }
func (f *fakeBackend) TmxSend(_ context.Context, _ *coreauth.Auth, method, url string, _ map[string]string, body []byte) (int, []byte, error) {
	f.mu.Lock()
	f.calls = append(f.calls, method+" "+url)
	if body != nil {
		f.bodies = append(f.bodies, string(body))
	}
	f.mu.Unlock()
	code, resp, err := f.handle(method, url, body)
	return code, []byte(resp), err
}
func (f *fakeBackend) TmxResetCooldown(_ context.Context, id string) error {
	f.cooldown = append(f.cooldown, id)
	return nil
}
func (f *fakeBackend) TmxClearQuotaSignals(id string) bool {
	f.signals = append(f.signals, id)
	return true
}

func codexAuth() *coreauth.Auth {
	return &coreauth.Auth{ID: "codex-1", Provider: "codex", Label: "codex@example", Metadata: map[string]any{"account_id": "acct-1"}}
}

func claudeAuth() *coreauth.Auth {
	return &coreauth.Auth{ID: "claude-1", Provider: "claude", Label: "claude@example",
		Metadata: map[string]any{"organization_uuid": "0F24A0D0-1111-2222-3333-444455556666"}}
}

func newTestService(f *fakeBackend) *Service {
	s := New(f, newMemJournal())
	s.now = func() time.Time { return testNow }
	return s
}

func TestCodexRedeemIsDeterministicAndConfirmed(t *testing.T) {
	used := 80.0
	f := &fakeBackend{auths: []*coreauth.Auth{codexAuth()}}
	f.handle = func(method, url string, _ []byte) (int, string, error) {
		switch {
		case strings.HasSuffix(url, "/wham/usage"):
			return 200, `{"rate_limit": {"primary_window": {"used_percent": ` + jsonNum(used) + `, "limit_window_seconds": 604800, "reset_at": 1793102375}}, "rate_limit_reset_credits": {"applicable_available_count": 1}}`, nil
		case strings.HasSuffix(url, "/rate-limit-reset-credits"):
			return 200, codexCreditsBody, nil
		case strings.HasSuffix(url, "/consume"):
			used = 0
			return 200, `{"code": "reset"}`, nil
		}
		return 500, "", nil
	}
	s := newTestService(f)
	res, err := s.Redeem(context.Background(), "codex-1", "c1", "test")
	if err != nil {
		t.Fatal(err)
	}
	r := res.Redemption
	if r.State != "settled" || r.Outcome != OutcomeReset || !r.Confirmed || r.Warning != "" {
		t.Fatalf("redemption %+v", r)
	}
	if len(f.cooldown) != 1 || len(f.signals) != 1 {
		t.Fatalf("cooldown/signals not cleared: %v %v", f.cooldown, f.signals)
	}
	var sent map[string]string
	_ = json.Unmarshal([]byte(f.bodies[0]), &sent)
	if sent["credit_id"] != "c1" || sent["redeem_request_id"] == "" {
		t.Fatalf("consume body %v", sent)
	}
	first := sent["redeem_request_id"]
	// A second service (another process, another tab) derives the same id for the same credit.
	f.bodies = nil
	used = 50
	if _, err = newTestService(f).Redeem(context.Background(), "codex-1", "c1", "test"); err != nil {
		t.Fatal(err)
	}
	_ = json.Unmarshal([]byte(f.bodies[0]), &sent)
	if sent["redeem_request_id"] != first {
		t.Fatalf("codex redeem id must be deterministic: %s vs %s", sent["redeem_request_id"], first)
	}
}

func TestClaudeUnknownOutcomeStaysPendingAndRetriesSameID(t *testing.T) {
	answer := `{"result": "unavailable"}`
	f := &fakeBackend{auths: []*coreauth.Auth{claudeAuth()}}
	f.handle = func(method, url string, _ []byte) (int, string, error) {
		if strings.Contains(url, "/reset_rate_limits") {
			if !strings.Contains(url, "/0f24a0d0-1111-2222-3333-444455556666/") {
				t.Errorf("org uuid not normalised: %s", url)
			}
			return 200, answer, nil
		}
		return 200, claudeBody, nil
	}
	s := newTestService(f)
	res, err := s.Redeem(context.Background(), "claude-1", "launch_reset", "test")
	if err != nil {
		t.Fatal(err)
	}
	if res.Redemption.State != "pending" || res.Redemption.Outcome != OutcomeUnknown || len(f.cooldown) != 0 {
		t.Fatalf("unknown outcome must stay pending without touching cooldown: %+v", res.Redemption)
	}
	if res.Account == nil || res.Account.Pending == nil {
		t.Fatal("account must carry the pending claim")
	}
	accts := s.Accounts(context.Background(), false)
	if len(accts) != 1 || accts[0].Pending == nil {
		t.Fatalf("Accounts must show the pending claim: %+v", accts)
	}

	// A refusal on retry can't prove the first claim didn't spend: still pending, same id.
	answer = `{"result": "not_limited"}`
	if res, err = s.Redeem(context.Background(), "claude-1", "launch_reset", "test"); err != nil {
		t.Fatal(err)
	}
	if res.Redemption.State != "pending" || res.Redemption.Outcome != OutcomeNothingToReset {
		t.Fatalf("refusal on retry: %+v", res.Redemption)
	}
	answer = `{"result": "already_used"}`
	if res, err = s.Redeem(context.Background(), "claude-1", "launch_reset", "test"); err != nil {
		t.Fatal(err)
	}
	if res.Redemption.State != "settled" || res.Redemption.Outcome != OutcomeAlreadyRedeemed {
		t.Fatalf("already_used settles: %+v", res.Redemption)
	}
	ids := map[string]bool{}
	for _, b := range f.bodies {
		var sent map[string]string
		_ = json.Unmarshal([]byte(b), &sent)
		if sent["program"] != "cedar_ember" || sent["grant_id"] != "launch_reset" {
			t.Fatalf("claim body %v", sent)
		}
		ids[sent["request_id"]] = true
	}
	if len(f.bodies) != 3 || len(ids) != 1 {
		t.Fatalf("all retries must reuse one request id: %d bodies, %d ids", len(f.bodies), len(ids))
	}
}

func TestRedeemRefusesBlockedOrUnknownCredit(t *testing.T) {
	f := &fakeBackend{auths: []*coreauth.Auth{claudeAuth()}}
	f.handle = func(string, string, []byte) (int, string, error) { return 200, claudeBody, nil }
	s := newTestService(f)
	var blocked *BlockedError
	if _, err := s.Redeem(context.Background(), "claude-1", "later_one", "t"); !errors.As(err, &blocked) || blocked.Code != "not_limited" {
		t.Fatalf("want not_limited, got %v", err)
	}
	if _, err := s.Redeem(context.Background(), "claude-1", "nope", "t"); !errors.As(err, &blocked) || blocked.Code != "unknown_credit" {
		t.Fatalf("want unknown_credit, got %v", err)
	}
	if _, err := s.Redeem(context.Background(), "missing", "x", "t"); !errors.Is(err, ErrUnknownAccount) {
		t.Fatalf("want unknown account, got %v", err)
	}
	for _, c := range f.calls {
		if strings.HasPrefix(c, "POST") {
			t.Fatalf("no claim may be sent: %v", f.calls)
		}
	}
}

func TestRedeemBusyWhileAnotherRuns(t *testing.T) {
	f := &fakeBackend{auths: []*coreauth.Auth{codexAuth()}}
	s := newTestService(f)
	lock := s.lockFor("codex-1")
	lock.Lock()
	defer lock.Unlock()
	f.handle = func(string, string, []byte) (int, string, error) { return 200, "{}", nil }
	if _, err := s.Redeem(context.Background(), "codex-1", "c1", "t"); !errors.Is(err, ErrBusy) {
		t.Fatalf("want busy, got %v", err)
	}
}

func TestAccountsCachesAndKeepsCreditsOnFailedRead(t *testing.T) {
	fail := false
	f := &fakeBackend{auths: []*coreauth.Auth{codexAuth(), {ID: "gem", Provider: "gemini"}}}
	f.handle = func(_, url string, _ []byte) (int, string, error) {
		if fail {
			return 429, "", nil
		}
		if strings.HasSuffix(url, "/wham/usage") {
			return 200, codexUsageBody, nil
		}
		return 200, codexCreditsBody, nil
	}
	s := newTestService(f)
	accts := s.Accounts(context.Background(), false)
	if len(accts) != 1 || len(accts[0].Credits) != 2 || accts[0].AuthIndex == "" {
		t.Fatalf("accounts %+v", accts)
	}
	calls := len(f.calls)
	s.Accounts(context.Background(), true) // within 30 s: served from cache
	if len(f.calls) != calls {
		t.Fatal("refresh within 30 s must not call upstream")
	}
	fail = true
	later := testNow.Add(cacheTTL)
	s.now = func() time.Time { return later }
	accts = s.Accounts(context.Background(), false)
	if accts[0].Error != "rate_limited" || len(accts[0].Credits) != 2 {
		t.Fatalf("a failed re-read keeps the last credits and reports the error: %+v", accts[0])
	}
}

func TestConfirmed(t *testing.T) {
	p := func(f float64) *float64 { return &f }
	before := []Window{{Window: "5h", UsedPct: p(60)}, {Window: "7d", UsedPct: p(40)}}
	if !confirmed(before, []Window{{Window: "5h", UsedPct: p(0)}, {Window: "7d", UsedPct: p(1)}}, []string{"5h", "7d"}) {
		t.Fatal("cleared windows must confirm")
	}
	if confirmed(before, []Window{{Window: "5h", UsedPct: p(60)}, {Window: "7d", UsedPct: p(0)}}, []string{"5h", "7d"}) {
		t.Fatal("an unchanged cleared window must not confirm")
	}
	if !confirmed(before, []Window{{Window: "5h", UsedPct: p(60)}, {Window: "7d", UsedPct: p(0)}}, []string{"7d"}) {
		t.Fatal("windows the credit doesn't clear are ignored")
	}
	if confirmed(before, nil, nil) {
		t.Fatal("no reading, no confirmation")
	}
}

func jsonNum(f float64) string {
	b, _ := json.Marshal(f)
	return string(b)
}
