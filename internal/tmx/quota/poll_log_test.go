package quota

import (
	"fmt"
	"strings"
	"testing"
	"time"

	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	log "github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
)

type missingWindowsObserver struct{ missing []string }

func (o *missingWindowsObserver) ApplyQuotaObservation(string, string, []coreauth.QuotaWindowObservation) bool {
	return false
}

func (o *missingWindowsObserver) TmxOverThresholdWindowsBefore(string, time.Time) []string {
	return o.missing
}

// pollLogEntries returns the poll-state entries since the last reset, checking each carries only
// fixed codes, the provider and the opaque auth index.
func pollLogEntries(t *testing.T, hook *logtest.Hook, auth *coreauth.Auth) []*log.Entry {
	t.Helper()
	allowed := map[string]bool{"code": true, "provider": true, "auth_index": true, "reason": true, "window": true}
	var out []*log.Entry
	for _, e := range hook.AllEntries() {
		if !strings.HasPrefix(fmt.Sprint(e.Data["code"]), "tmx_quota_poll_") {
			continue
		}
		for key, value := range e.Data {
			if !allowed[key] {
				t.Fatalf("poll log carries field %q", key)
			}
			if text := fmt.Sprint(value); strings.Contains(text, "alice") || strings.Contains(text, auth.ID) {
				t.Fatalf("poll log leaks an identity in %q: %q", key, text)
			}
		}
		if e.Data["auth_index"] != auth.EnsureIndex() || e.Data["provider"] != "claude" {
			t.Fatalf("poll log fields = %v", e.Data)
		}
		out = append(out, e)
	}
	hook.Reset()
	return out
}

func wantPollLog(t *testing.T, entries []*log.Entry, want ...string) {
	t.Helper()
	got := make([]string, 0, len(entries))
	for _, e := range entries {
		label := e.Level.String() + " " + fmt.Sprint(e.Data["code"])
		if reason, ok := e.Data["reason"]; ok {
			label += " reason=" + fmt.Sprint(reason)
		}
		if window, ok := e.Data["window"]; ok {
			label += " window=" + fmt.Sprint(window)
		}
		got = append(got, label)
	}
	if strings.Join(got, "; ") != strings.Join(want, "; ") {
		t.Fatalf("poll logs = %q, want %q", got, want)
	}
}

// A failed poll warns once per change of failure, and a successful poll that lacks a window
// routing holds as over threshold warns once until that window comes back.
func TestPollStateLogsOncePerChange(t *testing.T) {
	hook := logtest.NewGlobal()
	defer hook.Reset()
	observer := &missingWindowsObserver{}
	s := &Service{observer: observer}
	a := &account{}
	auth := &coreauth.Auth{ID: "claude-hash-alice@example.com.json", Provider: "Claude", Label: "alice@example.com"}
	now := time.Now()

	s.notePollFailure(auth, a, "rate_limited")
	wantPollLog(t, pollLogEntries(t, hook, auth), "warning tmx_quota_poll_failed reason=rate_limited")
	s.notePollFailure(auth, a, "rate_limited")
	s.notePollFailure(auth, a, "rate_limited")
	wantPollLog(t, pollLogEntries(t, hook, auth))
	s.notePollFailure(auth, a, "unauthorized")
	wantPollLog(t, pollLogEntries(t, hook, auth), "warning tmx_quota_poll_failed reason=unauthorized")

	s.notePollSuccess(auth, a, now)
	wantPollLog(t, pollLogEntries(t, hook, auth), "info tmx_quota_poll_recovered reason=unauthorized")
	s.notePollSuccess(auth, a, now)
	wantPollLog(t, pollLogEntries(t, hook, auth))

	observer.missing = []string{"7d"}
	s.notePollSuccess(auth, a, now)
	wantPollLog(t, pollLogEntries(t, hook, auth), "warning tmx_quota_poll_missing_window window=7d")
	s.notePollSuccess(auth, a, now)
	wantPollLog(t, pollLogEntries(t, hook, auth))
	// A failure in between keeps the missing window's state: no repeat warning after it.
	s.notePollFailure(auth, a, "http_status")
	s.notePollSuccess(auth, a, now)
	wantPollLog(t, pollLogEntries(t, hook, auth), "warning tmx_quota_poll_failed reason=http_status", "info tmx_quota_poll_recovered reason=http_status")

	observer.missing = []string{"7d", "7d_oi"}
	s.notePollSuccess(auth, a, now)
	wantPollLog(t, pollLogEntries(t, hook, auth), "warning tmx_quota_poll_missing_window window=7d_oi")
	observer.missing = nil
	s.notePollSuccess(auth, a, now)
	wantPollLog(t, pollLogEntries(t, hook, auth), "info tmx_quota_poll_window_back window=7d", "info tmx_quota_poll_window_back window=7d_oi")
	s.notePollSuccess(auth, a, now)
	wantPollLog(t, pollLogEntries(t, hook, auth))
}

// End to end through poll() and the real routing view: a usage reply without seven_day, while
// routing holds 7d at 98%, warns once (and never for windows routing doesn't weigh); a reply that
// carries it again clears the state.
func TestPollWarnsOnMissingOverThresholdWindow(t *testing.T) {
	// The service's clock runs behind real time: routing (real time) accepts every poll's reading,
	// and the clock can step past the poll gaps without reaching the future.
	now := time.Now().UTC().Truncate(time.Second).Add(-2 * time.Hour)
	auth := &coreauth.Auth{ID: "claude-hash-alice@example.com.json", Provider: "claude", Status: coreauth.StatusActive}
	f := &fakeFetcher{auths: []*coreauth.Auth{auth}, calls: map[string]int{}, status: 200}
	s := openTestService(t, f, &now)
	m := coreauth.NewManager(nil, &coreauth.ResetSoonestSelector{}, nil)
	if _, err := m.Register(t.Context(), auth.Clone()); err != nil {
		t.Fatal(err)
	}
	s.observer = m
	full := 98.0
	reset := now.Add(40 * time.Hour)
	// 7d_oauth_apps and 7d_cowork are over threshold too, but routing weighs them for no model, so
	// a poll that omits them must not warn about them.
	var seeded []coreauth.QuotaWindowObservation
	for _, w := range []string{"7d", "7d_oauth_apps", "7d_cowork"} {
		seeded = append(seeded, coreauth.QuotaWindowObservation{Window: w, UsedPercent: &full, ResetsAt: reset, ObservedAt: now.Add(-time.Hour)})
	}
	m.ApplyQuotaObservation(auth.ID, "claude", seeded)
	hook := logtest.NewGlobal()
	defer hook.Reset()

	f.usage = fmt.Sprintf(`{"five_hour":{"utilization":10,"resets_at":%q}}`, now.Add(2*time.Hour).Format(time.RFC3339))
	for i := 0; i < 2; i++ {
		now = now.Add(pollNowMinGap + time.Second)
		if _, _, err := s.PollNow(t.Context(), auth.ID); err != nil {
			t.Fatal(err)
		}
	}
	if f.calls[claudeUsageURL] != 2 {
		t.Fatalf("usage calls = %d, want 2", f.calls[claudeUsageURL])
	}
	wantPollLog(t, pollLogEntries(t, hook, auth), "warning tmx_quota_poll_missing_window window=7d")

	now = now.Add(pollNowMinGap + time.Second)
	f.usage = fmt.Sprintf(`{"five_hour":{"utilization":10,"resets_at":%q},"seven_day":{"utilization":98,"resets_at":%q}}`,
		now.Add(2*time.Hour).Format(time.RFC3339), reset.Format(time.RFC3339))
	if _, _, err := s.PollNow(t.Context(), auth.ID); err != nil {
		t.Fatal(err)
	}
	wantPollLog(t, pollLogEntries(t, hook, auth), "info tmx_quota_poll_window_back window=7d")

	f.status = 429
	for i := 0; i < 2; i++ {
		now = now.Add(rateLimitPause + time.Second)
		_, _, _ = s.PollNow(t.Context(), auth.ID)
	}
	if f.calls[claudeUsageURL] != 5 {
		t.Fatalf("usage calls = %d, want 5", f.calls[claudeUsageURL])
	}
	wantPollLog(t, pollLogEntries(t, hook, auth), "warning tmx_quota_poll_failed reason=rate_limited")
}
