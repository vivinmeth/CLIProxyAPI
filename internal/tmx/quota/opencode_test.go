package quota

import (
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/opencode"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/requestlog"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

// The reply seen from the real API on 2026-11-04 (matches the OpenCode console).
const openCodeUsageReply = `{"usage":{"rolling":{"status":"ok","percent":2,"resetsAt":"2026-11-04T08:36:27.000Z"},` +
	`"weekly":{"status":"ok","percent":42,"resetsAt":"2026-11-09T00:00:00.000Z"},` +
	`"monthly":{"status":"rate-limited","percent":100,"resetsAt":"2026-12-03T13:32:12.000Z"}}}`

func TestParseOpenCodeUsage(t *testing.T) {
	ws, err := ParseOpenCodeUsage([]byte(openCodeUsageReply))
	if err != nil || len(ws) != 3 {
		t.Fatalf("windows = %+v, %v", ws, err)
	}
	want := []struct {
		window, reset, status string
		pct                   float64
	}{
		{"5h", "2026-11-04T08:36:27Z", opencode.StatusAllowed, 2},
		{"7d", "2026-11-09T00:00:00Z", opencode.StatusAllowed, 42},
		{"30d", "2026-12-03T13:32:12Z", opencode.StatusAtLimit, 100},
	}
	for i, w := range want {
		got := ws[i]
		if got.Window != w.window || got.UsedPct == nil || *got.UsedPct != w.pct || got.ResetsAt == nil ||
			got.ResetsAt.Format(time.RFC3339) != w.reset || got.Status != w.status {
			t.Fatalf("window %d = %+v (used %v reset %v), want %+v", i, got, got.UsedPct, got.ResetsAt, w)
		}
	}
	// 0 is "under 1%": a reading with its reset, not an absent one. "ok" at 100 stays OpenCode's ok.
	ws, err = ParseOpenCodeUsage([]byte(`{"usage":{"rolling":{"status":"ok","percent":0,"resetsAt":"2026-11-04T08:36:27Z"},` +
		`"weekly":{"status":"ok","percent":100,"resetsAt":"2026-11-09T00:00:00Z"},"monthly":{"status":"ok","percent":85,"resetsAt":"2026-12-03T13:32:12Z"}}}`))
	if err != nil || ws[0].UsedPct == nil || *ws[0].UsedPct != 0 || ws[0].ResetsAt == nil ||
		ws[1].Status != opencode.StatusWarning || ws[2].Status != opencode.StatusWarning {
		t.Fatalf("windows = %+v, %v", ws, err)
	}
	for _, bad := range []string{
		`{}`, `not json`, `{"usage":{"rolling":{"status":"ok","percent":2,"resetsAt":"2026-11-04T08:36:27Z"}}}`,
		`{"usage":{"rolling":{"status":"ok","resetsAt":"x"},"weekly":{},"monthly":{}}}`,
		`{"usage":{"rolling":{"status":"ok","percent":2,"resetsAt":"soon"},"weekly":{"percent":1,"resetsAt":"2026-11-09T00:00:00Z"},"monthly":{"percent":1,"resetsAt":"2026-11-09T00:00:00Z"}}}`,
		`{"usage":{"rolling":{"percent":-1,"resetsAt":"2026-11-04T08:36:27Z"},"weekly":{"percent":1,"resetsAt":"2026-11-09T00:00:00Z"},"monthly":{"percent":1,"resetsAt":"2026-11-09T00:00:00Z"}}}`,
		// A status OpenCode's schema doesn't have (or none) is a changed API.
		`{"usage":{"rolling":{"status":"limited","percent":2,"resetsAt":"2026-11-04T08:36:27Z"},"weekly":{"status":"ok","percent":1,"resetsAt":"2026-11-09T00:00:00Z"},"monthly":{"status":"ok","percent":1,"resetsAt":"2026-11-09T00:00:00Z"}}}`,
		`{"usage":{"rolling":{"percent":2,"resetsAt":"2026-11-04T08:36:27Z"},"weekly":{"status":"ok","percent":1,"resetsAt":"2026-11-09T00:00:00Z"},"monthly":{"status":"ok","percent":1,"resetsAt":"2026-11-09T00:00:00Z"}}}`,
	} {
		if _, err = ParseOpenCodeUsage([]byte(bad)); err == nil {
			t.Fatalf("accepted %s", bad)
		}
	}
}

func TestMergeOpenCode(t *testing.T) {
	now := time.Date(2026, 11, 4, 6, 0, 0, 0, time.UTC)
	f := func(v float64) *float64 { return &v }
	tm := func(d time.Duration) *time.Time { t := now.Add(d); return &t }
	poll := &openCodePoll{at: now.Add(-5 * time.Minute), windows: []Window{
		{Window: "5h", UsedPct: f(2), ResetsAt: tm(-time.Minute), Status: opencode.StatusAllowed}, // reset since the poll
		{Window: "7d", UsedPct: f(42), ResetsAt: tm(72 * time.Hour), Status: opencode.StatusAllowed},
		{Window: "30d", UsedPct: f(100), ResetsAt: tm(700 * time.Hour), Status: opencode.StatusAtLimit},
	}}
	passive := []opencode.WindowReading{
		{Window: "5h", UsedUSD: 1.2, CapUSD: f(12), UsedPct: f(10), ResetsAt: tm(4 * time.Hour), Status: opencode.StatusAllowed},
		{Window: "7d", UsedUSD: 3, CapUSD: f(30), UsedPct: f(10), Status: opencode.StatusAllowed},
		{Window: "30d", UsedUSD: 3, CapUSD: f(60), UsedPct: f(5), Status: opencode.StatusAllowed},
	}
	byName := func(ws []Window) map[string]Window {
		out := map[string]Window{}
		for _, w := range ws {
			out[w.Window] = w
		}
		return out
	}

	// Nothing logged since the poll: only the window whose polled reset passed (allowance estimate).
	got := byName(mergeOpenCode(poll, passive, 0, now))
	if len(got) != 1 || *got["5h"].UsedPct != 10 || !got["5h"].ResetsAt.Equal(*tm(4 * time.Hour)) {
		t.Fatalf("merge without new allowance = %+v", got)
	}

	// $15 since the poll: 7d = 42% + 15/30 = 92% (warning), 30d stays rejected (OpenCode said so).
	got = byName(mergeOpenCode(poll, passive, 15, now))
	if w := got["7d"]; len(got) != 3 || *w.UsedPct != 92 || w.Status != opencode.StatusWarning || !w.ResetsAt.Equal(*tm(72 * time.Hour)) {
		t.Fatalf("7d = %+v", got)
	}
	if w := got["30d"]; *w.UsedPct != 125 || w.Status != opencode.StatusAtLimit {
		t.Fatalf("30d = %+v", w)
	}

	// No poll yet (or only failed ones): the allowance reading as it was.
	got = byName(mergeOpenCode(nil, passive, 0, now))
	if len(got) != 3 || *got["7d"].UsedPct != 10 || got["7d"].ResetsAt != nil {
		t.Fatalf("fallback = %+v", got)
	}

	// Unknown pool: the poll stands alone.
	got = byName(mergeOpenCode(poll, []opencode.WindowReading{{Window: "7d", Status: opencode.StatusUnknownCap}}, 15, now))
	if len(got) != 0 {
		t.Fatalf("unknown cap = %+v", got)
	}
}

func TestOpenCodeCredentials(t *testing.T) {
	cred := func(id, entry, key string, disabled bool) *coreauth.Auth {
		return &coreauth.Auth{ID: id, Provider: "openai-compatible-" + entry, Disabled: disabled,
			Attributes: map[string]string{"compat_name": entry, "api_key": key, "base_url": "https://opencode.ai/inference/go/openai/v1"}}
	}
	auths := []*coreauth.Auth{
		cred("b", "opencode-go", "oc_sk_one", false),
		cred("a", "opencode-go", "oc_sk_two", false),
		cred("c", "opencode-go", "oc_sk_three", true),
		cred("d", "opencode-zen", "oc_sk_one", false),
		cred("e", "opencode-go", "oc_sk_stray", false),
		{ID: "claude.json", Provider: "claude"},
	}
	st := opencode.Settings{Entry: "opencode-go", Accounts: []opencode.Account{
		{Name: "account-1", Keys: []string{requestlog.KeyFingerprint("oc_sk_one")}},
		{Name: "account-2", Keys: []string{requestlog.KeyFingerprint("oc_sk_two")}},
		{Name: "account-3", Keys: []string{requestlog.KeyFingerprint("oc_sk_three")}},
	}}
	got := openCodeCredentials(auths, st)
	if len(got) != 2 || got["account-1"].ID != "b" || got["account-2"].ID != "a" {
		t.Fatalf("credentials = %+v", got)
	}
	// No accounts: the entry's first credential by id.
	st.Accounts = nil
	if got = openCodeCredentials(auths, st); len(got) != 1 || got[""].ID != "a" {
		t.Fatalf("credentials without accounts = %+v", got)
	}
}

// A polled window that has reset brings the next poll forward once; a failed re-read then waits for
// the interval instead of retrying every tick.
func TestOpenCodePollDueAfterReset(t *testing.T) {
	now := time.Date(2026, 11, 4, 8, 40, 0, 0, time.UTC)
	s := &Service{pollEvery: 10 * time.Minute}
	reset := time.Date(2026, 11, 4, 8, 36, 27, 0, time.UTC)
	a := &account{Account: Account{AuthID: "opencode:opencode-go:account-1"},
		ocPoll: &openCodePoll{windows: []Window{{Window: "5h", ResetsAt: &reset}}}}
	last := reset.Add(-5 * time.Minute)
	a.LastPollAt = &last
	if !s.openCodePollDueLocked(a, now) {
		t.Fatal("a reset since the last poll did not bring the poll forward")
	}
	last = reset.Add(time.Minute) // a poll after the reset failed (kept the old window)
	if s.openCodePollDueLocked(a, now) {
		t.Fatal("polling again every tick after a failed re-read")
	}
	if !s.openCodePollDueLocked(a, last.Add(11*time.Minute)) {
		t.Fatal("the interval no longer polls")
	}
}
