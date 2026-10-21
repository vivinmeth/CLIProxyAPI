package auth

import (
	"context"
	"net/http"
	"strconv"
	"testing"
	"time"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

var resetSoonestTestNow = time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)

func unixString(t time.Time) string { return strconv.FormatInt(t.Unix(), 10) }

// observedAuth builds an auth whose quota snapshot comes from the real header observation path.
func observedAuth(t *testing.T, id, provider string, headers http.Header, observedAt time.Time) *Auth {
	t.Helper()
	auth := &Auth{ID: id, Provider: provider, Status: StatusActive}
	if headers != nil && !auth.Quota.ObserveResponseHeadersForProvider(provider, headers, observedAt) {
		t.Fatalf("ObserveResponseHeadersForProvider(%s) recorded no signals", provider)
	}
	return auth
}

func claudeWeeklyHeaders(reset7d, reset7dOI time.Time) http.Header {
	headers := http.Header{}
	headers.Set("anthropic-ratelimit-unified-status", "allowed")
	headers.Set("anthropic-ratelimit-unified-5h-status", "allowed")
	headers.Set("anthropic-ratelimit-unified-5h-reset", unixString(resetSoonestTestNow.Add(2*time.Hour)))
	if !reset7d.IsZero() {
		headers.Set("anthropic-ratelimit-unified-7d-status", "allowed")
		headers.Set("anthropic-ratelimit-unified-7d-reset", unixString(reset7d))
	}
	if !reset7dOI.IsZero() {
		headers["anthropic-ratelimit-unified-7d_oi-reset"] = []string{unixString(reset7dOI)}
	}
	return headers
}

func codexWeeklyHeaders(secondaryReset time.Time) http.Header {
	headers := http.Header{}
	headers.Set("x-codex-primary-used-percent", "10")
	headers.Set("x-codex-primary-window-minutes", "300")
	headers.Set("x-codex-primary-reset-at", unixString(resetSoonestTestNow.Add(time.Hour)))
	if !secondaryReset.IsZero() {
		headers.Set("x-codex-secondary-used-percent", "40")
		headers.Set("x-codex-secondary-window-minutes", "10080")
		headers.Set("x-codex-secondary-reset-at", unixString(secondaryReset))
	}
	return headers
}

func TestWeeklyResetAtClaude(t *testing.T) {
	now := resetSoonestTestNow
	in3d, in5d := now.Add(72*time.Hour), now.Add(120*time.Hour)

	cases := []struct {
		name    string
		headers http.Header
		want    time.Time
		wantOK  bool
	}{
		{"shared 7d", claudeWeeklyHeaders(in5d, time.Time{}), in5d, true},
		{"model 7d earlier", claudeWeeklyHeaders(in5d, in3d), in3d, true},
		{"shared 7d earlier", claudeWeeklyHeaders(in3d, in5d), in3d, true},
		{"rolled model window ignored", claudeWeeklyHeaders(in5d, now.Add(-time.Hour)), in5d, true},
		{"only 5h window", claudeWeeklyHeaders(time.Time{}, time.Time{}), time.Time{}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			auth := observedAuth(t, "claude-a", "claude", tc.headers, now)
			got, ok := weeklyResetAt(auth, now)
			if ok != tc.wantOK || !got.Equal(tc.want) {
				t.Fatalf("weeklyResetAt() = %v, %v; want %v, %v", got, ok, tc.want, tc.wantOK)
			}
		})
	}
}

func TestWeeklyResetAtClaudePastReset(t *testing.T) {
	now := resetSoonestTestNow
	past := now.Add(-time.Hour)
	auth := observedAuth(t, "claude-a", "claude", claudeWeeklyHeaders(past, time.Time{}), now.Add(-7*24*time.Hour))
	got, ok := weeklyResetAt(auth, now)
	if ok {
		t.Fatalf("weeklyResetAt() = %v, %v; past reset must be stale", got, ok)
	}
}

func TestWeeklyResetAtCodex(t *testing.T) {
	now := resetSoonestTestNow
	in4d := now.Add(96 * time.Hour)

	t.Run("secondary reset-at", func(t *testing.T) {
		auth := observedAuth(t, "codex-a", "codex", codexWeeklyHeaders(in4d), now)
		got, ok := weeklyResetAt(auth, now)
		if !ok || !got.Equal(in4d) {
			t.Fatalf("weeklyResetAt() = %v, %v; want %v, true", got, ok, in4d)
		}
	})

	t.Run("secondary reset-after-seconds", func(t *testing.T) {
		headers := http.Header{}
		headers.Set("x-codex-secondary-used-percent", "40")
		headers.Set("x-codex-secondary-window-minutes", "10080")
		headers.Set("x-codex-secondary-reset-after-seconds", "3600")
		observedAt := now.Add(-30 * time.Minute)
		auth := observedAuth(t, "codex-a", "codex", headers, observedAt)
		got, ok := weeklyResetAt(auth, now)
		if want := observedAt.Add(time.Hour); !ok || !got.Equal(want) {
			t.Fatalf("weeklyResetAt() = %v, %v; want %v, true", got, ok, want)
		}
	})

	t.Run("primary 5h window only", func(t *testing.T) {
		auth := observedAuth(t, "codex-a", "codex", codexWeeklyHeaders(time.Time{}), now)
		if got, ok := weeklyResetAt(auth, now); ok {
			t.Fatalf("weeklyResetAt() = %v, true; want unknown", got)
		}
	})

	t.Run("weekly primary window", func(t *testing.T) {
		headers := http.Header{}
		headers.Set("x-codex-primary-used-percent", "10")
		headers.Set("x-codex-primary-window-minutes", "10080")
		headers.Set("x-codex-primary-reset-at", unixString(in4d))
		auth := observedAuth(t, "codex-a", "codex", headers, now)
		got, ok := weeklyResetAt(auth, now)
		if !ok || !got.Equal(in4d) {
			t.Fatalf("weeklyResetAt() = %v, %v; want %v, true", got, ok, in4d)
		}
	})
}

func TestWeeklyResetAtUnobserved(t *testing.T) {
	now := resetSoonestTestNow
	for _, auth := range []*Auth{
		nil,
		{ID: "gemini", Provider: "gemini"},
		{ID: "claude", Provider: "claude", Quota: QuotaState{Signals: map[string]string{"Anthropic-Ratelimit-Unified-7d-Reset": "garbage"}}},
	} {
		if got, ok := weeklyResetAt(auth, now); ok {
			t.Fatalf("weeklyResetAt(%v) = %v, true; want unknown", auth, got)
		}
	}
}

func TestWeeklyResetAtDevin(t *testing.T) {
	now := resetSoonestTestNow
	reset := now.Add(48 * time.Hour)
	auth := &Auth{ID: "devin", Provider: "devin", Quota: QuotaState{ObservedAt: now, Signals: map[string]string{
		"weekly_quota_reset_at": reset.Format(time.RFC3339),
	}}}
	got, ok := weeklyResetAt(auth, now)
	if !ok || !got.Equal(reset) {
		t.Fatalf("weeklyResetAt() = %v, %v; want %v, true", got, ok, reset)
	}
}

func newTestResetSoonestSelector(now time.Time) *ResetSoonestSelector {
	return &ResetSoonestSelector{nowFunc: func() time.Time { return now }}
}

func pickIDs(t *testing.T, selector Selector, provider string, auths []*Auth) []string {
	t.Helper()
	remaining := append([]*Auth(nil), auths...)
	var order []string
	for len(remaining) > 0 {
		got, err := selector.Pick(context.Background(), provider, "", cliproxyexecutor.Options{}, remaining)
		if err != nil {
			t.Fatalf("Pick() error = %v", err)
		}
		order = append(order, got.ID)
		next := remaining[:0]
		for _, auth := range remaining {
			if auth.ID != got.ID {
				next = append(next, auth)
			}
		}
		remaining = next
	}
	return order
}

func TestResetSoonestSelectorOrdering(t *testing.T) {
	now := resetSoonestTestNow
	auths := []*Auth{
		observedAuth(t, "a-past", "claude", claudeWeeklyHeaders(now.Add(-time.Hour), time.Time{}), now.Add(-6*24*time.Hour)),
		observedAuth(t, "b-5d", "claude", claudeWeeklyHeaders(now.Add(120*time.Hour), time.Time{}), now),
		observedAuth(t, "c-unknown", "claude", nil, time.Time{}),
		observedAuth(t, "d-2d", "claude", claudeWeeklyHeaders(now.Add(48*time.Hour), time.Time{}), now),
		observedAuth(t, "e-2d-tie", "claude", claudeWeeklyHeaders(now.Add(48*time.Hour), time.Time{}), now),
		observedAuth(t, "f-unknown", "claude", nil, time.Time{}),
	}
	got := pickIDs(t, newTestResetSoonestSelector(now), "claude", auths)
	want := []string{"c-unknown", "f-unknown", "d-2d", "e-2d-tie", "b-5d", "a-past"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("pick order = %v, want %v", got, want)
		}
	}
}

func TestResetSoonestSelectorClockDrivesRollover(t *testing.T) {
	now := resetSoonestTestNow
	soon := observedAuth(t, "a-soon", "codex", codexWeeklyHeaders(now.Add(time.Hour)), now)
	later := observedAuth(t, "b-later", "codex", codexWeeklyHeaders(now.Add(72*time.Hour)), now)
	auths := []*Auth{soon, later}

	got, err := newTestResetSoonestSelector(now).Pick(context.Background(), "codex", "", cliproxyexecutor.Options{}, auths)
	if err != nil || got.ID != "a-soon" {
		t.Fatalf("Pick() = %v, %v; want a-soon", got, err)
	}
	// After a-soon's window rolls over, its new reset is about a week away.
	got, err = newTestResetSoonestSelector(now.Add(2*time.Hour)).Pick(context.Background(), "codex", "", cliproxyexecutor.Options{}, auths)
	if err != nil || got.ID != "b-later" {
		t.Fatalf("Pick() after rollover = %v, %v; want b-later", got, err)
	}
}

func TestResetSoonestSelectorRespectsPriority(t *testing.T) {
	now := resetSoonestTestNow
	high := observedAuth(t, "a-high-late", "claude", claudeWeeklyHeaders(now.Add(140*time.Hour), time.Time{}), now)
	high.Attributes = map[string]string{"priority": "10"}
	low := observedAuth(t, "b-low-soon", "claude", claudeWeeklyHeaders(now.Add(time.Hour), time.Time{}), now)
	lowUnknown := observedAuth(t, "c-low-unknown", "claude", nil, time.Time{})

	got, err := newTestResetSoonestSelector(now).Pick(context.Background(), "claude", "", cliproxyexecutor.Options{}, []*Auth{lowUnknown, low, high})
	if err != nil || got.ID != "a-high-late" {
		t.Fatalf("Pick() = %v, %v; want a-high-late", got, err)
	}
}

func TestResetSoonestSelectorSkipsUnavailable(t *testing.T) {
	now := resetSoonestTestNow
	const model = "claude-sonnet"
	cooling := observedAuth(t, "a-cooling", "claude", claudeWeeklyHeaders(now.Add(time.Hour), time.Time{}), now)
	cooling.ModelStates = map[string]*ModelState{model: {
		Status:         StatusActive,
		Unavailable:    true,
		NextRetryAfter: now.Add(30 * time.Minute),
		Quota:          QuotaState{Exceeded: true, NextRecoverAt: now.Add(30 * time.Minute)},
	}}
	disabled := observedAuth(t, "b-disabled", "claude", claudeWeeklyHeaders(now.Add(2*time.Hour), time.Time{}), now)
	disabled.Disabled = true
	disabled.Status = StatusDisabled
	ready := observedAuth(t, "c-ready", "claude", claudeWeeklyHeaders(now.Add(96*time.Hour), time.Time{}), now)

	selector := newTestResetSoonestSelector(now)
	got, err := selector.Pick(context.Background(), "claude", model, cliproxyexecutor.Options{}, []*Auth{cooling, disabled, ready})
	if err != nil || got.ID != "c-ready" {
		t.Fatalf("Pick() = %v, %v; want c-ready", got, err)
	}
	// Once the cooldown elapses the soonest-resetting credential is preferred again.
	selector = newTestResetSoonestSelector(now.Add(31 * time.Minute))
	got, err = selector.Pick(context.Background(), "claude", model, cliproxyexecutor.Options{}, []*Auth{cooling, disabled, ready})
	if err != nil || got.ID != "a-cooling" {
		t.Fatalf("Pick() after cooldown = %v, %v; want a-cooling", got, err)
	}
}

func TestResetSoonestSelectorWithoutObservationActsLikeFillFirst(t *testing.T) {
	auths := []*Auth{{ID: "b", Provider: "gemini"}, {ID: "a", Provider: "gemini"}, {ID: "c", Provider: "gemini"}}
	got, err := (&ResetSoonestSelector{}).Pick(context.Background(), "gemini", "", cliproxyexecutor.Options{}, auths)
	if err != nil || got.ID != "a" {
		t.Fatalf("Pick() = %v, %v; want a", got, err)
	}
}

func TestManagerResetSoonestUsesSelectorPath(t *testing.T) {
	ctx := context.Background()
	manager := NewManager(nil, &ResetSoonestSelector{}, nil)
	manager.executors["codex"] = schedulerTestExecutor{}
	if manager.useSchedulerFastPath() {
		t.Fatal("useSchedulerFastPath() = true, want selector path for reset-soonest")
	}
	now := time.Now()
	for _, auth := range []*Auth{
		{ID: "a-late", Provider: "codex", Status: StatusActive, Quota: QuotaState{ObservedAt: now, Signals: map[string]string{"X-Codex-Secondary-Window-Minutes": "10080", codexSecondaryResetAtSignal: unixString(now.Add(120 * time.Hour))}}},
		{ID: "b-soon", Provider: "codex", Status: StatusActive, Quota: QuotaState{ObservedAt: now, Signals: map[string]string{"X-Codex-Secondary-Window-Minutes": "10080", codexSecondaryResetAtSignal: unixString(now.Add(24 * time.Hour))}}},
	} {
		if _, errRegister := manager.Register(ctx, auth); errRegister != nil {
			t.Fatalf("Register(%s) error = %v", auth.ID, errRegister)
		}
	}
	got, _, errPick := manager.pickNext(ctx, "codex", "", cliproxyexecutor.Options{}, nil)
	if errPick != nil || got == nil || got.ID != "b-soon" {
		t.Fatalf("pickNext() = %v, %v; want b-soon", got, errPick)
	}
	got, _, _, errPick = manager.pickNextMixed(ctx, []string{"codex"}, "", cliproxyexecutor.Options{}, nil)
	if errPick != nil || got == nil || got.ID != "b-soon" {
		t.Fatalf("pickNextMixed() = %v, %v; want b-soon", got, errPick)
	}
	got, _, errPick = manager.pickNext(ctx, "codex", "", cliproxyexecutor.Options{}, map[string]struct{}{"b-soon": {}})
	if errPick != nil || got == nil || got.ID != "a-late" {
		t.Fatalf("pickNext(tried b-soon) = %v, %v; want a-late", got, errPick)
	}
}

func TestSessionAffinityBindingOutranksResetSoonest(t *testing.T) {
	ctx := context.Background()
	now := resetSoonestTestNow
	selector := NewSessionAffinitySelector(newTestResetSoonestSelector(now))
	t.Cleanup(selector.Stop)

	late := observedAuth(t, "a-late", "claude", claudeWeeklyHeaders(now.Add(120*time.Hour), time.Time{}), now)
	soon := observedAuth(t, "b-soon", "claude", claudeWeeklyHeaders(now.Add(24*time.Hour), time.Time{}), now)
	opts := cliproxyexecutor.Options{OriginalRequest: []byte(`{"session_id":"reset-soonest-session-1"}`)}

	// Cold binding follows the strategy while only the late credential is available.
	first, err := selector.Pick(ctx, "claude", "", opts, []*Auth{late})
	if err != nil || first.ID != "a-late" {
		t.Fatalf("first Pick() = %v, %v; want a-late", first, err)
	}
	// The established binding is kept even though b-soon now resets sooner.
	second, err := selector.Pick(ctx, "claude", "", opts, []*Auth{late, soon})
	if err != nil || second.ID != "a-late" {
		t.Fatalf("bound Pick() = %v, %v; want a-late", second, err)
	}
	// A request without a session uses reset-soonest ordering.
	other, err := selector.Pick(ctx, "claude", "", cliproxyexecutor.Options{}, []*Auth{late, soon})
	if err != nil || other.ID != "b-soon" {
		t.Fatalf("sessionless Pick() = %v, %v; want b-soon", other, err)
	}
}
