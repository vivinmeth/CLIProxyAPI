package resets

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"testing"
	"time"

	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

func ptrF(f float64) *float64 { return &f }
func at(d time.Duration) *time.Time {
	t := testNow.Add(d)
	return &t
}

func claudeAccount() Account {
	return Account{AuthID: "claude-1", Provider: "claude",
		Status: Status{Eligible: true, NextCreditID: "launch",
			Windows: []Window{{Window: "5h", UsedPct: ptrF(9), ResetsAt: at(80 * time.Minute)},
				{Window: "7d", UsedPct: ptrF(6), ResetsAt: at(158 * time.Hour)}},
			Credits: []Credit{{ID: "launch", ExpiresAt: at(48 * time.Hour), Clears: []string{"5h", "7d", "7d_oi"}, ResetsLeft: 1}}}}
}

func codexAccount() Account {
	return Account{AuthID: "codex-1", Provider: "codex", Status: Status{Eligible: true,
		Windows: []Window{{Window: "7d", UsedPct: ptrF(0), ResetsAt: at(163 * time.Hour)}},
		Credits: []Credit{
			{ID: "c3", ExpiresAt: at(17 * 24 * time.Hour), Clears: []string{"7d"}, ResetsLeft: 1, Warning: "nothing_to_clear"},
			{ID: "c1", ExpiresAt: at(50 * time.Hour), Clears: []string{"7d"}, ResetsLeft: 1, Warning: "nothing_to_clear"},
		}}}
}

func verdictOf(a Account, id string) *Verdict {
	decorate(&a, testNow)
	for _, c := range a.Credits {
		if c.ID == id {
			return c.Verdict
		}
	}
	return nil
}

func TestVerdictUseOrLose(t *testing.T) {
	v := verdictOf(claudeAccount(), "launch")
	if v.Kind != VerdictUseOrLose || v.TotalGain != 15 || !v.LapsesBeforeRefill || !v.LapsesSoon || !v.Spendable {
		t.Fatalf("verdict %+v", v)
	}
	if len(v.Gains) != 2 || v.Gains[0].Window != "5h" || v.Gains[0].UsedPct != 9 {
		t.Fatalf("gains %+v", v.Gains)
	}
}

func TestVerdictUseNowAndHold(t *testing.T) {
	a := claudeAccount()
	a.Windows[1].UsedPct = ptrF(100)
	if v := verdictOf(a, "launch"); v.Kind != VerdictUseNow {
		t.Fatalf("exhausted weekly far from refill: %+v", v)
	}
	a.Windows[1].ResetsAt = at(3 * time.Hour)
	a.Credits[0].ExpiresAt = at(10 * 24 * time.Hour)
	if v := verdictOf(a, "launch"); v.Kind != VerdictHold {
		t.Fatalf("refill within 12 h and credit outlives it: %+v", v)
	}
}

func TestVerdictNothingBlockedPendingAndSpendable(t *testing.T) {
	c := codexAccount()
	if v := verdictOf(c, "c1"); v.Kind != VerdictNothing || !v.Spendable || !v.LapsesBeforeRefill {
		t.Fatalf("codex c1: %+v", v)
	}
	if v := verdictOf(c, "c3"); v.Spendable {
		t.Fatal("only the soonest-expiring credit is spendable")
	}
	a := claudeAccount()
	a.Credits[0].Blocker = "not_limited"
	if v := verdictOf(a, "launch"); v.Kind != VerdictBlocked || v.Spendable {
		t.Fatalf("blocked: %+v", v)
	}
	a.Pending = &Redemption{CreditID: "launch", State: "pending"}
	if v := verdictOf(a, "launch"); v.Kind != VerdictPending {
		t.Fatalf("pending wins: %+v", v)
	}
}

func TestSummarize(t *testing.T) {
	s := Summarize([]Account{claudeAccount(), codexAccount()}, testNow)
	if s.Banked != 3 || s.LapseSoon != 2 || s.AccountsWithCredits != 2 || s.NextExpiry == nil ||
		s.NextExpiry.CreditID != "launch" || s.NextExpiry.AuthID != "claude-1" {
		t.Fatalf("summary %+v", s)
	}
}

func TestAccountsResponsesCarryVerdictsWithoutMutatingCache(t *testing.T) {
	f := &fakeBackend{auths: []*coreauth.Auth{claudeAuth()}}
	f.handle = func(string, string, []byte) (int, string, error) { return 200, claudeBody, nil }
	s := newTestService(f)
	accts := s.Accounts(context.Background(), false)
	if accts[0].Credits[0].Verdict == nil || accts[0].Credits[0].Verdict.Kind != VerdictUseOrLose {
		t.Fatalf("verdict missing: %+v", accts[0].Credits[0])
	}
	s.mu.Lock()
	cachedVerdict := s.cache["claude-1"].acct.Credits[0].Verdict
	s.mu.Unlock()
	if cachedVerdict != nil {
		t.Fatal("decorating a response must not write into the cache")
	}
}

func TestPollNowThrottles(t *testing.T) {
	f := &fakeBackend{auths: []*coreauth.Auth{claudeAuth()}}
	f.handle = func(string, string, []byte) (int, string, error) { return 200, claudeBody, nil }
	s := newTestService(f)
	acct, throttled, err := s.PollNow(context.Background(), "claude-1")
	if err != nil || throttled || acct == nil || len(acct.Credits) != 2 || acct.Credits[0].Verdict == nil {
		t.Fatalf("first poll: %v %v %+v", err, throttled, acct)
	}
	calls := len(f.calls)
	if _, throttled, _ = s.PollNow(context.Background(), "claude-1"); !throttled || len(f.calls) != calls {
		t.Fatal("second poll within 30 s must be throttled and make no provider call")
	}
	if _, _, err = s.PollNow(context.Background(), "nope"); err != ErrUnknownAccount {
		t.Fatalf("unknown account: %v", err)
	}
}

func TestHistoryFiltersAndPages(t *testing.T) {
	j := newMemJournal()
	s := New(&fakeBackend{}, j)
	for i, r := range []Redemption{
		{AuthID: "a", Provider: "codex", State: "settled", Outcome: OutcomeReset},
		{AuthID: "b", Provider: "claude", State: "pending", Outcome: OutcomeUnknown},
		{AuthID: "a", Provider: "codex", State: "settled", Outcome: OutcomeNothingToReset},
	} {
		r.CreatedAt = testNow.Add(time.Duration(i) * time.Minute)
		_ = j.insert(context.Background(), &r)
	}
	got, _ := s.History(context.Background(), HistoryQuery{AuthID: "a", Limit: 1})
	if len(got) != 1 || got[0].ID != 3 {
		t.Fatalf("newest first, limited: %+v", got)
	}
	got, _ = s.History(context.Background(), HistoryQuery{AuthID: "a", BeforeID: 3, Limit: 10})
	if len(got) != 1 || got[0].ID != 1 {
		t.Fatalf("paging: %+v", got)
	}
	got, _ = s.History(context.Background(), HistoryQuery{State: "pending"})
	if len(got) != 1 || got[0].AuthID != "b" {
		t.Fatalf("state filter: %+v", got)
	}
	got, _ = s.History(context.Background(), HistoryQuery{From: testNow.Add(90 * time.Second)})
	if len(got) != 1 || got[0].ID != 3 {
		t.Fatalf("from filter: %+v", got)
	}
}

// TestJSONContract pins the field names fleet and the UI read (wiki/reset-credits.md).
func TestJSONContract(t *testing.T) {
	a := claudeAccount()
	a.Pending = &Redemption{CreditID: "launch", State: "pending"}
	decorate(&a, testNow)
	b, _ := json.Marshal(a)
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	want := []string{"auth_id", "auth_index", "provider", "label", "plan", "checked_at", "eligible", "at_limit",
		"windows", "credits", "next_credit_id", "pending"}
	for _, k := range want {
		if _, ok := m[k]; !ok {
			t.Errorf("account missing %q", k)
		}
	}
	credit := m["credits"].([]any)[0].(map[string]any)
	for _, k := range []string{"id", "title", "expires_at", "clears", "resets_left", "verdict"} {
		if _, ok := credit[k]; !ok {
			t.Errorf("credit missing %q", k)
		}
	}
	verdict := credit["verdict"].(map[string]any)
	var keys []string
	for k := range verdict {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if got := strings.Join(keys, ","); got != "gains,kind,lapses_before_refill,lapses_soon,refill_at,spendable,total_gain" {
		t.Errorf("verdict keys: %s", got)
	}
	b, _ = json.Marshal(Redemption{ID: 1, RequestID: "secret-ish"})
	if strings.Contains(string(b), "secret-ish") || strings.Contains(string(b), "request_id") {
		t.Error("request ids must not be exposed")
	}
}
