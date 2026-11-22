package quota

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/MoeForge/moego/json"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

func xaiTestSpec(t *testing.T) providerSpec {
	t.Helper()
	spec, _, ok := specFor(&coreauth.Auth{ID: "synthetic", Provider: "xai"})
	if !ok {
		t.Fatal("xAI quota provider is not tracked")
	}
	return spec
}

func TestXAIParseBilling(t *testing.T) {
	spec := xaiTestSpec(t)
	for _, tc := range []struct {
		name, config, window string
		used                 *float64
		start, end           string
	}{
		{"weekly", `"creditUsagePercent":42.12345,"currentPeriod":{"type":"USAGE_PERIOD_TYPE_WEEKLY","start":"2026-11-01T12:34:56.123456+05:30","end":"2026-11-08T12:34:56.123456+05:30"}`, "7d", floatPtr(42.12345), "2026-11-01T12:34:56.123456+05:30", "2026-11-08T12:34:56.123456+05:30"},
		{"monthly", `"creditUsagePercent":100,"currentPeriod":{"type":"USAGE_PERIOD_TYPE_MONTHLY","start":"2026-02-01T00:00:00Z","end":"2026-03-01T00:00:00Z"}`, "monthly", floatPtr(100), "2026-02-01T00:00:00Z", "2026-03-01T00:00:00Z"},
		{"zero", `"creditUsagePercent":0`, "billing", floatPtr(0), "", ""},
		{"legacy", `"used":{"val":1234},"monthlyLimit":{"val":2000},"billingPeriodStart":"2026-11-01T00:00:00Z","billingPeriodEnd":"2026-12-01T00:00:00Z"`, "billing", floatPtr(61.7), "2026-11-01T00:00:00Z", "2026-12-01T00:00:00Z"},
		{"proto-zero", `"used":{},"monthlyLimit":{"val":2000}`, "billing", floatPtr(0), "", ""},
		{"missing-used", `"monthlyLimit":{"val":2000},"billingPeriodEnd":"2026-12-01T00:00:00Z"`, "billing", nil, "", "2026-12-01T00:00:00Z"},
		{"null-used", `"used":null,"monthlyLimit":{"val":2000},"currentPeriod":{"type":"USAGE_PERIOD_TYPE_WEEKLY"}`, "7d", nil, "", ""},
		{"zero-limit", `"used":{"val":25},"monthlyLimit":{},"currentPeriod":{"end":"2026-12-01T00:00:00Z"}`, "billing", nil, "", "2026-12-01T00:00:00Z"},
		{"prefer-credits", `"creditUsagePercent":12,"used":{"val":100},"monthlyLimit":{"val":200}`, "billing", floatPtr(12), "", ""},
		{"prefer-period", `"creditUsagePercent":12,"currentPeriod":{"type":"USAGE_PERIOD_TYPE_WEEKLY","end":"2026-11-08T00:00:00Z"},"billingPeriodStart":"2026-11-01T00:00:00Z","billingPeriodEnd":"2026-12-01T00:00:00Z"`, "7d", floatPtr(12), "", "2026-11-08T00:00:00Z"},
		{"unknown-period", `"creditUsagePercent":25,"currentPeriod":{"type":"FUTURE_PERIOD"}`, "billing", floatPtr(25), "", ""},
		{"separate-pools", `"creditUsagePercent":30,"onDemandCap":{"val":100},"onDemandUsed":{"val":90},"prepaidBalance":{"val":5000},"isUnifiedBillingUser":true,"productUsage":[{"usagePercent":99}]`, "billing", floatPtr(30), "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ws, plan, err := spec.parseUsage([]byte(`{"config":{` + tc.config + `}}`))
			if err != nil || len(ws) != 1 || plan != "" {
				t.Fatalf("windows=%+v plan=%q err=%v", ws, plan, err)
			}
			w := ws[0]
			if w.Window != tc.window || (w.UsedPct == nil) != (tc.used == nil) || w.UsedPct != nil && *w.UsedPct != *tc.used {
				t.Fatalf("reading=%+v", w)
			}
			for _, pair := range []struct {
				got  *time.Time
				want string
			}{{w.StartsAt, tc.start}, {w.ResetsAt, tc.end}} {
				if pair.want == "" {
					if pair.got != nil {
						t.Fatal("invented time", pair.got)
					}
					continue
				}
				want, _ := time.Parse(time.RFC3339Nano, pair.want)
				if pair.got == nil || !pair.got.Equal(want) {
					t.Fatalf("time=%v want=%v", pair.got, want)
				}
			}
			b, err := json.Marshal(w)
			if err != nil {
				t.Fatal(err)
			}
			if tc.start == "" && strings.Contains(string(b), "starts_at") {
				t.Fatal("unknown start serialized")
			}
		})
	}
}

func TestXAIRejectsMalformedAndUnknownBilling(t *testing.T) {
	spec := xaiTestSpec(t)
	for _, body := range []string{`not-json`, `null`, `[]`, `{}`, `{"config":null}`, `{"config":{}}`,
		`{"config":{"creditUsagePercent":null}}`, `{"config":{"prepaidBalance":{"val":500}}}`, `{"config":{"used":{"val":0},"monthlyLimit":{"val":0}}}`,
		`{"config":{"used":null,"monthlyLimit":{"val":2000}}}`, `{"config":{"creditUsagePercent":-1}}`, `{"config":{"creditUsagePercent":101}}`, `{"config":{"creditUsagePercent":"25"}}`, `{"config":{"creditUsagePercent":true}}`, `{"config":{"creditUsagePercent":1e999}}`,
		`{"config":{"used":{"val":-1},"monthlyLimit":{"val":100}}}`, `{"config":{"used":{"val":10},"monthlyLimit":{"val":-100}}}`, `{"config":{"used":{"val":1.5},"monthlyLimit":{"val":100}}}`, `{"config":{"used":{"val":"10"},"monthlyLimit":{"val":100}}}`, `{"config":{"used":{"val":null},"monthlyLimit":{"val":100}}}`,
		`{"config":{"used":{"val":9223372036854775808},"monthlyLimit":{"val":100}}}`, `{"config":{"creditUsagePercent":25,"currentPeriod":{"start":"tomorrow"}}}`, `{"config":{"creditUsagePercent":25,"currentPeriod":{"end":1000}}}`, `{"config":{"creditUsagePercent":25,"currentPeriod":{"start":"2026-12-01T00:00:00Z","end":"2026-11-01T00:00:00Z"}}}`,
		`{"config":{"creditUsagePercent":25,"currentPeriod":{"start":"2026-12-01T00:00:00Z","end":"2026-12-01T00:00:00Z"}}}`,
	} {
		t.Run(body, func(t *testing.T) {
			ws, _, err := spec.parseUsage([]byte(body))
			if err == nil || len(ws) != 0 {
				t.Fatalf("bad evidence: %+v %v", ws, err)
			}
		})
	}
}

func floatPtr(f float64) *float64 { return &f }

// Generic fetches are a hard failure: xAI must never reach token resolution.
type xaiFetcher struct {
	auths   []*coreauth.Auth
	calls   map[string]int
	replies map[string]fakeRoute
}

func (f *xaiFetcher) TmxAuths() []*coreauth.Auth { return f.auths }
func (f *xaiFetcher) TmxFetch(context.Context, *coreauth.Auth, string, map[string]string) (int, []byte, error) {
	panic("generic refresh-capable fetch")
}
func (f *xaiFetcher) TmxXAIBilling(_ context.Context, a *coreauth.Auth) (int, []byte, *coreauth.Auth, error) {
	f.calls[a.ID]++
	r := f.replies[a.ID]
	return r.status, []byte(r.body), a.Clone(), nil
}
func xaiQuotaAuth(id string) *coreauth.Auth {
	return &coreauth.Auth{ID: id, Provider: "xai", Status: coreauth.StatusActive, RegistrationEpoch: 1, CredentialVersion: 1}
}

func TestXAIQuotaSchedulerAndRefusal(t *testing.T) {
	for _, status := range []int{400, 401, 403, 404, 426, 429, 500} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			xaiTestSpec(t)
			now := time.Date(2026, 11, 22, 0, 0, 0, 0, time.UTC)
			a, b := xaiQuotaAuth("bad"), xaiQuotaAuth("healthy")
			f := &xaiFetcher{auths: []*coreauth.Auth{a, b}, calls: map[string]int{}, replies: map[string]fakeRoute{a.ID: {200, `{"config":{"creditUsagePercent":30}}`}, b.ID: {200, `{"config":{"creditUsagePercent":50}}`}}}
			s := agRefusalService(t, f, &now)
			s.tick(context.Background())
			first := s.Accounts()[0].Windows[0].ObservedAt
			s.tick(context.Background())
			if f.calls[a.ID] != 1 {
				t.Fatal("cadence bypass")
			}
			now = now.Add(11 * time.Minute)
			f.replies[a.ID] = fakeRoute{status, `{"error":"private-synthetic-detail"}`}
			s.tick(context.Background())
			bad := s.Accounts()[0]
			if bad.PollError == "" || strings.Contains(bad.PollError, "private") || !bad.Windows[0].ObservedAt.Equal(first) {
				t.Fatal("failure corrupted evidence", bad)
			}
			now = now.Add(time.Minute)
			_, throttled, err := s.PollNow(context.Background(), a.ID)
			if err != nil {
				t.Fatal(err)
			}
			if status < 500 && !throttled {
				t.Fatal("4xx not backed off")
			}
			if status == 401 {
				if bad.NextPollAt != nil {
					t.Fatal("auth refusal advertises retry")
				}
				a.CredentialVersion++
				a.Metadata = map[string]any{"access_token": "synthetic-rotated"}
				for _, delta := range []time.Duration{2 * time.Hour, 24 * time.Hour} {
					now = now.Add(delta)
					s.tick(context.Background())
					_, throttled, err = s.PollNow(context.Background(), a.ID)
					if err != nil || !throttled || f.calls[a.ID] != 2 {
						t.Fatal("auth refusal silently rearmed")
					}
				}
				if f.calls[b.ID] < 4 {
					t.Fatal("refusal stopped healthy account")
				}
			}
		})
	}
}

func TestXAIDisabledAPIKeyAndNoPassiveSubscription(t *testing.T) {
	spec := xaiTestSpec(t)
	for _, a := range []*coreauth.Auth{{Provider: "xai", Disabled: true}, {Provider: "xai", Status: coreauth.StatusDisabled}, {Provider: "xai", Attributes: map[string]string{"api_key": "synthetic"}}, {Provider: "xai", Attributes: map[string]string{"source": "config", "auth_kind": "api_key"}}} {
		if _, _, ok := specFor(a); ok {
			t.Fatalf("unsupported auth tracked: %+v", a)
		}
	}
	if ws := spec.passive(map[string]string{"X-Ratelimit-Limit-Requests": "100", "X-Ratelimit-Remaining-Tokens": "0", "X-Ratelimit-Reset": "10"}, time.Now()); len(ws) != 0 {
		t.Fatal("request limits became subscription windows")
	}
	f := &fakeFetcher{}
	_, _, err := (&Service{fetch: f}).fetchUsage(context.Background(), spec, xaiQuotaAuth("no-interface"))
	if err == nil {
		t.Fatal("missing current-token interface did not fail closed")
	}
}

func TestXAIQuotaUnknownsTransitionsAndRecovery(t *testing.T) {
	xaiTestSpec(t)
	now := time.Date(2026, 11, 22, 0, 0, 0, 0, time.UTC)
	a := xaiQuotaAuth("period-transition")
	f := &xaiFetcher{auths: []*coreauth.Auth{a}, calls: map[string]int{}, replies: map[string]fakeRoute{a.ID: {200, `{"config":{"creditUsagePercent":25,"currentPeriod":{"type":"USAGE_PERIOD_TYPE_WEEKLY","start":"2026-11-22T00:00:00Z","end":"2026-11-29T00:00:00Z"}}}`}}}
	s := agRefusalService(t, f, &now)
	s.tick(context.Background())
	now = now.Add(11 * time.Minute)
	f.replies[a.ID] = fakeRoute{200, `{"config":{"currentPeriod":{"type":"USAGE_PERIOD_TYPE_MONTHLY","end":"2026-12-22T00:00:00Z"}}}`}
	s.tick(context.Background())
	acc := s.Accounts()[0]
	if len(acc.Windows) != 1 || acc.Windows[0].Window.Window != "monthly" || acc.Windows[0].UsedPct != nil || acc.Windows[0].StartsAt != nil || acc.PollError != "" {
		t.Fatal("unknowns or new period lost", acc)
	}
	observed := acc.Windows[0].ObservedAt
	now = now.Add(11 * time.Minute)
	f.replies[a.ID] = fakeRoute{200, `{"config":null}`}
	s.tick(context.Background())
	acc = s.Accounts()[0]
	if acc.PollError == "" || !acc.Windows[0].ObservedAt.Equal(observed) {
		t.Fatal("malformed data became fresh")
	}
	now = now.Add(11 * time.Minute)
	f.replies[a.ID] = fakeRoute{429, ""}
	s.tick(context.Background())
	before := f.calls[a.ID]
	now = now.Add(29 * time.Minute)
	s.tick(context.Background())
	if f.calls[a.ID] != before {
		t.Fatal("429 pause bypassed")
	}
	now = now.Add(2 * time.Minute)
	f.replies[a.ID] = fakeRoute{401, ""}
	s.tick(context.Background())
	before = f.calls[a.ID]
	a.Disabled = true
	s.tick(context.Background())
	if len(s.Accounts()) != 0 {
		t.Fatal("disabled account exposed")
	}
	a.Disabled = false
	now = now.Add(24 * time.Hour)
	s.tick(context.Background())
	if f.calls[a.ID] != before {
		t.Fatal("disable/re-enable rearmed auth refusal")
	}
	a.RegistrationEpoch++
	now = now.Add(11 * time.Minute)
	f.replies[a.ID] = fakeRoute{200, `{"config":{"creditUsagePercent":0}}`}
	s.tick(context.Background())
	acc = s.Accounts()[0]
	if f.calls[a.ID] != before+1 || acc.PollError != "" || len(acc.Windows) != 1 || acc.Windows[0].Window.Window != "billing" || *acc.Windows[0].UsedPct != 0 {
		t.Fatal("explicit registration recovery failed", acc)
	}
}

type xaiSentOverride struct {
	*xaiFetcher
	sent  *coreauth.Auth
	after func()
}

func (f *xaiSentOverride) TmxXAIBilling(ctx context.Context, a *coreauth.Auth) (int, []byte, *coreauth.Auth, error) {
	st, b, _, err := f.xaiFetcher.TmxXAIBilling(ctx, a)
	if f.after != nil {
		f.after()
	}
	return st, b, f.sent, err
}
func TestXAIQuotaRejectsStaleOrMissingSentIdentity(t *testing.T) {
	for _, mode := range []string{"missing", "other-account", "version", "epoch", "disabled", "rotated-after-read"} {
		t.Run(mode, func(t *testing.T) {
			xaiTestSpec(t)
			now := time.Date(2026, 11, 22, 0, 0, 0, 0, time.UTC)
			a := xaiQuotaAuth("sent-" + mode)
			base := &xaiFetcher{auths: []*coreauth.Auth{a}, calls: map[string]int{}, replies: map[string]fakeRoute{a.ID: {401, ""}}}
			f := &xaiSentOverride{xaiFetcher: base, sent: a.Clone()}
			switch mode {
			case "missing":
				f.sent = nil
			case "other-account":
				f.sent.ID = "other"
			case "version":
				f.sent.CredentialVersion++
			case "epoch":
				f.sent.RegistrationEpoch++
			case "disabled":
				f.after = func() { a.Disabled = true }
			case "rotated-after-read":
				f.after = func() { a.CredentialVersion++ }
			}
			s := agRefusalService(t, f, &now)
			s.tick(context.Background())
			if s.accounts[a.ID].refused != nil || len(s.accounts[a.ID].current) != 0 || s.accounts[a.ID].PollError == "" {
				t.Fatal("unattributed response accepted")
			}
		})
	}
}
