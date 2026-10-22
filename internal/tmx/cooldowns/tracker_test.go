package cooldowns

import (
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

func win(authID, model string, retry time.Time) auth.CooldownWindowState {
	return auth.CooldownWindowState{AuthID: authID, Model: model, RetryAt: retry, HTTPStatus: 429, Reason: "quota"}
}

func kinds(evs []Event) []string {
	out := make([]string, len(evs))
	for i, e := range evs {
		out[i] = e.Kind
		if e.Kind == KindEnd {
			out[i] += ":" + e.EndReason
		}
	}
	return out
}

func eq(t *testing.T, got []Event, want ...string) {
	t.Helper()
	k := kinds(got)
	if len(k) != len(want) {
		t.Fatalf("events %v, want %v", k, want)
	}
	for i := range k {
		if k[i] != want[i] {
			t.Fatalf("events %v, want %v", k, want)
		}
	}
}

func TestTrackerWindowLifecycle(t *testing.T) {
	tr := newTracker()
	t0 := time.Date(2026, 10, 22, 10, 0, 0, 0, time.UTC)

	start := tr.apply(auth.CooldownReport{Seq: 1, At: t0, AuthID: "a", Windows: []auth.CooldownWindowState{win("a", "m", t0.Add(time.Minute))}}, SourceProxy)
	eq(t, start, KindStart)
	id := start[0].CooldownID
	if id == "" || start[0].StartedAt == nil || !start[0].StartedAt.Equal(t0) || start[0].RetryAt == nil {
		t.Fatalf("start: %+v", start[0])
	}
	// Same state again: nothing.
	eq(t, tr.apply(auth.CooldownReport{Seq: 2, At: t0.Add(time.Second), AuthID: "a", Windows: []auth.CooldownWindowState{win("a", "m", t0.Add(time.Minute))}}, SourceProxy))
	// Extended: update with the same cooldown id.
	up := tr.apply(auth.CooldownReport{Seq: 3, At: t0.Add(2 * time.Second), AuthID: "a", Windows: []auth.CooldownWindowState{win("a", "m", t0.Add(5*time.Minute))}}, SourceProxy)
	eq(t, up, KindUpdate)
	if up[0].CooldownID != id {
		t.Fatal("update must keep the cooldown id")
	}
	// A stale report (lower seq) is ignored.
	eq(t, tr.apply(auth.CooldownReport{Seq: 2, At: t0.Add(3 * time.Second), AuthID: "a"}, SourceProxy))
	// Cleared before retry_at.
	end := tr.apply(auth.CooldownReport{Seq: 4, At: t0.Add(time.Minute), AuthID: "a"}, SourceProxy)
	eq(t, end, KindEnd+":"+EndCleared)
	if end[0].CooldownID != id || !end[0].EndedAt.Equal(t0.Add(time.Minute)) || !end[0].StartedAt.Equal(t0) {
		t.Fatalf("end: %+v", end[0])
	}
}

func TestTrackerExpiryAndRestart(t *testing.T) {
	tr := newTracker()
	t0 := time.Date(2026, 10, 22, 10, 0, 0, 0, time.UTC)
	tr.apply(auth.CooldownReport{Seq: 1, At: t0, AuthID: "a", Windows: []auth.CooldownWindowState{win("a", "", t0.Add(time.Minute))}}, SourceProxy)
	eq(t, tr.expire(t0.Add(30*time.Second)))
	exp := tr.expire(t0.Add(2 * time.Minute))
	eq(t, exp, KindEnd+":"+EndExpired)
	if !exp[0].EndedAt.Equal(t0.Add(time.Minute)) {
		t.Fatalf("expired window ends at retry_at: %v", exp[0].EndedAt)
	}

	// A new cooldown on a key whose window expired but wasn't swept: end + start.
	tr.apply(auth.CooldownReport{Seq: 2, At: t0, AuthID: "b", Windows: []auth.CooldownWindowState{win("b", "m", t0.Add(time.Minute))}}, SourceProxy)
	eq(t, tr.apply(auth.CooldownReport{Seq: 3, At: t0.Add(2 * time.Minute), AuthID: "b", Windows: []auth.CooldownWindowState{win("b", "m", t0.Add(3*time.Minute))}}, SourceProxy),
		KindEnd+":"+EndExpired, KindStart)

	// Restart: resume b and an older duplicate, plus c which is gone after the restart.
	tr2 := newTracker()
	resumed := tr2.resume([]*openWindow{
		{id: "old", startedAt: t0.Add(-time.Hour), state: win("b", "m", t0.Add(time.Hour))},
		{id: "new", startedAt: t0, state: win("b", "m", t0.Add(time.Hour))},
		{id: "c1", startedAt: t0, state: win("c", "", t0.Add(time.Hour))},
	}, t0.Add(time.Minute))
	eq(t, resumed, KindEnd+":"+EndSuperseded)
	snap := auth.CooldownReport{Seq: 10, At: t0.Add(time.Minute), Windows: []auth.CooldownWindowState{win("b", "m", t0.Add(time.Hour)), win("d", "", t0.Add(time.Hour))}}
	settled := tr2.settleRestart(snap)
	eq(t, settled, KindEnd+":"+EndRestart)
	if settled[0].CooldownID != "c1" {
		t.Fatalf("settled: %+v", settled[0])
	}
	started := tr2.apply(snap, SourceReconcile)
	eq(t, started, KindStart)
	if started[0].AuthID != "d" || started[0].Source != SourceReconcile || tr2.open[windowKey{"b", "m"}].id != "new" {
		t.Fatalf("resume kept the wrong window: %+v", started)
	}
}

func TestTrackerSnapshotOrdering(t *testing.T) {
	tr := newTracker()
	t0 := time.Date(2026, 10, 22, 10, 0, 0, 0, time.UTC)
	// A per-auth report newer than the snapshot is applied first (it raced ahead).
	eq(t, tr.apply(auth.CooldownReport{Seq: 5, At: t0, AuthID: "a", Windows: []auth.CooldownWindowState{win("a", "m", t0.Add(time.Hour))}}, SourceProxy), KindStart)
	// The snapshot (seq 4) predates it for auth a: a's window must not be ended.
	eq(t, tr.apply(auth.CooldownReport{Seq: 4, At: t0, Windows: []auth.CooldownWindowState{win("x", "", t0.Add(time.Hour))}}, SourceReconcile), KindStart)
	if _, ok := tr.open[windowKey{"a", "m"}]; !ok {
		t.Fatal("snapshot older than a report ended its window")
	}
	// A per-auth report older than the snapshot is ignored.
	eq(t, tr.apply(auth.CooldownReport{Seq: 3, At: t0, AuthID: "x"}, SourceProxy))
	// A newer snapshot settles everything.
	eq(t, tr.apply(auth.CooldownReport{Seq: 6, At: t0.Add(time.Second)}, SourceReconcile), KindEnd+":"+EndCleared, KindEnd+":"+EndCleared)
}

func TestTrackerReplaysSpooledEvents(t *testing.T) {
	tr := newTracker()
	t0 := time.Date(2026, 10, 22, 10, 0, 0, 0, time.UTC)
	evs := tr.apply(auth.CooldownReport{Seq: 1, At: t0, AuthID: "a", Windows: []auth.CooldownWindowState{win("a", "m", t0.Add(time.Hour)), win("a", "n", t0.Add(time.Hour))}}, SourceProxy)
	evs = append(evs, tr.apply(auth.CooldownReport{Seq: 2, At: t0, AuthID: "a", Windows: []auth.CooldownWindowState{win("a", "m", t0.Add(time.Hour))}}, SourceProxy)...)
	replay := newTracker()
	for _, ev := range evs {
		replay.applyEvent(ev)
	}
	if len(replay.open) != 1 || replay.open[windowKey{"a", "m"}].id != tr.open[windowKey{"a", "m"}].id {
		t.Fatalf("replayed open set: %+v", replay.open)
	}
}

func TestFailureKind(t *testing.T) {
	cases := []struct {
		f    auth.FailureReport
		want string
	}{
		{auth.FailureReport{HTTPStatus: 429, Message: "rate_limit_error: slow down"}, KindRateLimited},
		{auth.FailureReport{HTTPStatus: 429, Message: `{"error":{"type":"usage_limit_reached"}}`}, KindUsageLimit},
		{auth.FailureReport{HTTPStatus: 429, CredentialScope: true}, KindUsageLimit},
		{auth.FailureReport{HTTPStatus: 402}, KindUsageLimit},
		{auth.FailureReport{HTTPStatus: 401}, KindAuthError},
		{auth.FailureReport{HTTPStatus: 403}, KindAuthError},
		{auth.FailureReport{HTTPStatus: 400, Message: "invalid_grant"}, KindAuthError},
		{auth.FailureReport{HTTPStatus: 529}, KindOverloaded},
		{auth.FailureReport{HTTPStatus: 500}, ""},
	}
	for _, c := range cases {
		if got := FailureKind(c.f); got != c.want {
			t.Errorf("%+v: %q, want %q", c.f, got, c.want)
		}
	}
	d := 30 * time.Second
	ev, ok := failureEvent(auth.FailureReport{At: time.Now(), AuthID: "a", HTTPStatus: 429, RetryAfter: &d})
	if !ok || ev.RetryAfterMs != 30000 || ev.RetryAt == nil || ev.CooldownID != "" {
		t.Fatalf("failure event: %+v", ev)
	}
}

func TestCursorRoundTrip(t *testing.T) {
	ts := time.Date(2026, 10, 22, 10, 0, 0, 123456000, time.UTC)
	gotTS, gotID, err := decodeCursor(encodeCursor(ts, 42))
	if err != nil || !gotTS.Equal(ts) || gotID != 42 {
		t.Fatalf("%v %v %v", gotTS, gotID, err)
	}
	if _, _, err = decodeCursor("!!"); err != ErrBadCursor {
		t.Fatal("bad cursor accepted")
	}
}
