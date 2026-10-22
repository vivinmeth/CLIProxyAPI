package quota

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/muse"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

// The event fixture from meta_executor_test.go (TestMetaExecutor_FiltersSubscriptionUsageGlobally).
const metaEventFixture = `{"subscription":{"tier":"1234567890123456","weekly":{"resets_at":1792368000,"used_percent":80},` +
	`"window":{"resets_at":1792272519,"used_percent":10,"window_duration_mins":300}},"type":"response.subscription_usage"}`

func metaSignals(t *testing.T, event string) map[string]string {
	t.Helper()
	h := muse.SubscriptionUsageHeaders([]byte(event))
	out := map[string]string{}
	for k, v := range h {
		out[k] = v[0]
	}
	return out
}

func TestParseMetaSignalsFromEvent(t *testing.T) {
	ws := ParseMetaSignals(metaSignals(t, metaEventFixture), time.Time{})
	if len(ws) != 2 || ws[0].Window != "5h" || ws[1].Window != "7d" {
		t.Fatalf("windows = %+v", ws)
	}
	if *ws[0].UsedPct != 10 || ws[0].ResetsAt.Unix() != 1792272519 || *ws[1].UsedPct != 80 || ws[1].ResetsAt.Unix() != 1792368000 {
		t.Fatalf("values = %+v %+v", ws[0], ws[1])
	}
	other := strings.Replace(metaEventFixture, `"window_duration_mins":300`, `"window_duration_mins":240`, 1)
	if ws = ParseMetaSignals(metaSignals(t, other), time.Time{}); ws[0].Window != "240m" {
		t.Fatalf("240-minute window = %q, want 240m", ws[0].Window)
	}
	if ParseMetaSignals(metaSignals(t, `{"type":"response.subscription_usage","subscription":{}}`), time.Time{}) != nil {
		t.Fatal("empty usage must give no windows")
	}
}

func TestParseMetaKeyUsage(t *testing.T) {
	body := `{"api_key":"LLM|secret","subs_tier_id":"x","subs_tier_name":"Muse Code Everyday Usage",
		"subs_usage":{"window":{"used_percent":2,"window_duration_mins":300,"resets_at":1789678120},
		"weekly":{"used_percent":0,"resets_at":1789948800},"tier":"x"}}`
	ws, plan, err := ParseMetaKeyUsage([]byte(body))
	if err != nil || plan != "everyday" || len(ws) != 2 || ws[0].Window != "5h" || *ws[0].UsedPct != 2 ||
		ws[1].Window != "7d" || *ws[1].UsedPct != 0 || ws[1].ResetsAt.Unix() != 1789948800 {
		t.Fatalf("ws=%+v plan=%q err=%v", ws, plan, err)
	}
	// Before the account's first request there is no subs_usage: no windows, not an error.
	if ws, _, err = ParseMetaKeyUsage([]byte(`{"subs_tier_name":"High Usage"}`)); err != nil || len(ws) != 0 {
		t.Fatalf("no usage: ws=%v err=%v", ws, err)
	}
}

func TestMetaPlan(t *testing.T) {
	for in, want := range map[string]string{
		"Everyday Usage": "everyday", "High Usage": "high", "Muse Code Power Usage": "power",
		"Maximum  Usage": "maximum", "Pro": "", "": "", "Ultra High Usage Plus": "", "1234567890123456": "",
	} {
		if got := MetaPlan(in); got != want {
			t.Errorf("MetaPlan(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestMetaKeyURLFollowsBaseURL(t *testing.T) {
	if got := metaKeyURL(&coreauth.Auth{}); got != "https://api.meta.ai/muse-code/key" {
		t.Fatalf("default = %s", got)
	}
	a := &coreauth.Auth{Metadata: map[string]any{"base_url": "http://10.0.0.1:8399/v1"}}
	if got := metaKeyURL(a); got != "http://10.0.0.1:8399/muse-code/key" {
		t.Fatalf("stub = %s", got)
	}
}

type metaFetcher struct {
	fakeFetcher
	rmu    sync.Mutex
	posts  int
	method string
	body   string
	auth   string
}

func (f *metaFetcher) TmxRequest(_ context.Context, _ *coreauth.Auth, method, url string, headers map[string]string, body []byte) (int, []byte, error) {
	f.rmu.Lock()
	defer f.rmu.Unlock()
	f.posts++
	f.method, f.body, f.auth = method, string(body), headers["Authorization"]
	if url != "http://stub/muse-code/key" {
		return 404, nil, nil
	}
	return 200, []byte(`{"api_key":"LLM|minted","subs_tier_name":"High Usage","subs_usage":{"window":{"used_percent":12,` +
		`"window_duration_mins":300,"resets_at":1792520400},"weekly":{"used_percent":3,"resets_at":1792972800}}}`), nil
}

func TestQuotaServiceMetaPassivePollAndPlan(t *testing.T) {
	clock := time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)
	meta := &coreauth.Auth{ID: "meta-a.json", Provider: "meta", Label: "m@example.com",
		Metadata: map[string]any{"dca_token": "dca:fake", "base_url": "http://stub/v1"}}
	meta.Quota.ObserveResponseHeadersForProvider("meta", muse.SubscriptionUsageHeaders([]byte(metaEventFixture)), clock.Add(-time.Minute))
	keyOnly := &coreauth.Auth{ID: "meta-key.json", Provider: "meta", Metadata: map[string]any{"api_key": "LLM|x", "subs_tier_name": "Power Usage"}}
	f := &metaFetcher{fakeFetcher: fakeFetcher{auths: []*coreauth.Auth{meta, keyOnly}, calls: map[string]int{}}}
	s := openTestService(t, &f.fakeFetcher, &clock)
	s.fetch = f
	if _, err := s.db.Exec(`DELETE FROM ` + s.table); err != nil {
		t.Fatal(err)
	}
	s.accounts = map[string]*account{}
	ctx := context.Background()

	s.tick(ctx)
	if f.posts != 1 || f.method != "POST" || f.body != "{}" || f.auth != "Bearer dca:fake" {
		t.Fatalf("poll = %d %s %q %q", f.posts, f.method, f.body, f.auth)
	}
	accs := s.Accounts()
	if len(accs) != 2 {
		t.Fatalf("accounts = %+v", accs)
	}
	a, k := accs[0], accs[1]
	if a.Provider != "meta" || a.Plan != "high" || a.PollError != "" || a.LastPollAt == nil || len(a.Windows) != 2 {
		t.Fatalf("meta account = %+v", a)
	}
	if w := a.Windows[0]; w.Window.Window != "5h" || *w.UsedPct != 12 || w.Source != "polled" {
		t.Fatalf("5h = %+v", w)
	}
	if w := a.Windows[1]; w.Window.Window != "7d" || *w.UsedPct != 3 || w.Source != "polled" {
		t.Fatalf("7d = %+v", w)
	}
	hist, _, err := s.History(ctx, a.AuthID, "", clock.Add(-time.Hour), clock.Add(time.Hour), 100)
	if err != nil || len(hist) != 4 { // passive 5h/7d, then polled 5h/7d
		t.Fatalf("history = %+v err=%v", hist, err)
	}
	if hist[0].Source != "passive" || hist[0].Window != "5h" || *hist[0].UsedPct != 10 {
		t.Fatalf("first snapshot = %+v", hist[0])
	}
	// No DCA token: never polled; the plan comes from the login's saved tier name.
	if k.Plan != "power" || k.LastPollAt != nil || k.NextPollAt != nil || k.PollError != "" {
		t.Fatalf("key-only account = %+v", k)
	}
	got, throttled, err := s.PollNow(ctx, keyOnly.ID)
	if err != nil || !throttled || got.AuthID != keyOnly.ID || f.posts != 1 {
		t.Fatalf("PollNow key-only = %+v %v %v (posts %d)", got, throttled, err, f.posts)
	}
	clock = clock.Add(time.Minute)
	if got, throttled, err = s.PollNow(ctx, meta.ID); err != nil || throttled || f.posts != 2 || got.Plan != "high" {
		t.Fatalf("PollNow meta = %+v %v %v (posts %d)", got, throttled, err, f.posts)
	}
}
