package cooldowns

import (
	"sort"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

type windowKey struct{ authID, model string }

// openWindow is a cooldown that has a start event and no end event yet.
type openWindow struct {
	id        string
	startedAt time.Time
	state     auth.CooldownWindowState
}

// tracker turns cooldown reports into window events. It is used by one goroutine only.
type tracker struct {
	open    map[windowKey]*openWindow
	lastSeq map[string]uint64 // auth id -> Seq of the newest report applied for it
	floor   uint64            // Seq of the newest snapshot applied
}

func newTracker() *tracker {
	return &tracker{open: make(map[windowKey]*openWindow), lastSeq: make(map[string]uint64)}
}

// apply diffs a report against the open windows and returns the resulting events. Reports older
// than what the tracker has already seen for an auth are ignored (they were sent under the
// manager's lock but may arrive out of order relative to a snapshot).
func (t *tracker) apply(rep auth.CooldownReport, source string) []Event {
	at := rep.At
	if at.IsZero() {
		at = time.Now()
	}
	var inScope func(authID string) bool
	if rep.AuthID != "" {
		if rep.Seq != 0 && (rep.Seq <= t.floor || rep.Seq <= t.lastSeq[rep.AuthID]) {
			return nil
		}
		t.lastSeq[rep.AuthID] = rep.Seq
		inScope = func(authID string) bool { return authID == rep.AuthID }
	} else {
		if rep.Seq < t.floor {
			return nil
		}
		t.floor = rep.Seq
		// An auth whose newer report was already applied is fresher than this snapshot.
		inScope = func(authID string) bool { return t.lastSeq[authID] <= rep.Seq }
		for id, seq := range t.lastSeq {
			if seq <= rep.Seq {
				delete(t.lastSeq, id) // the floor covers it now
			}
		}
	}

	var out []Event
	live := make(map[windowKey]bool, len(rep.Windows))
	for _, w := range rep.Windows {
		if w.AuthID == "" || !inScope(w.AuthID) {
			continue
		}
		key := windowKey{w.AuthID, w.Model}
		live[key] = true
		cur := t.open[key]
		if cur != nil && !cur.state.RetryAt.After(at) {
			// The window we know expired; this one is a new cooldown.
			out = append(out, t.end(key, cur, cur.state.RetryAt, EndExpired, source))
			cur = nil
		}
		if cur == nil {
			ow := &openWindow{id: newKey(), startedAt: at, state: w}
			t.open[key] = ow
			out = append(out, windowEvent(KindStart, source, ow, at))
			continue
		}
		if changed(cur.state, w) {
			cur.state = w
			out = append(out, windowEvent(KindUpdate, source, cur, at))
		} else {
			cur.state.AuthIndex, cur.state.AuthLabel = w.AuthIndex, w.AuthLabel
		}
	}
	for _, key := range t.sortedKeys() {
		cur := t.open[key]
		if live[key] || !inScope(key.authID) {
			continue
		}
		if !cur.state.RetryAt.After(at) {
			out = append(out, t.end(key, cur, cur.state.RetryAt, EndExpired, source))
		} else {
			out = append(out, t.end(key, cur, at, EndCleared, source))
		}
	}
	return out
}

// expire ends windows whose retry_at has passed: the cooldown is over even if the manager hasn't
// touched the auth since.
func (t *tracker) expire(now time.Time) []Event {
	var out []Event
	for _, key := range t.sortedKeys() {
		if cur := t.open[key]; !cur.state.RetryAt.After(now) {
			out = append(out, t.end(key, cur, cur.state.RetryAt, EndExpired, SourceProxy))
		}
	}
	return out
}

// resume restores windows that were open before a restart (from the database and the spool).
// Duplicates for one auth/model keep the newest and end the rest.
func (t *tracker) resume(windows []*openWindow, now time.Time) []Event {
	sort.Slice(windows, func(i, j int) bool { return windows[i].startedAt.Before(windows[j].startedAt) })
	var out []Event
	for _, ow := range windows {
		key := windowKey{ow.state.AuthID, ow.state.Model}
		if prev := t.open[key]; prev != nil {
			out = append(out, t.end(key, prev, now, EndSuperseded, SourceReconcile))
		}
		t.open[key] = ow
	}
	return out
}

// settleRestart ends resumed windows that the first snapshot after a restart no longer shows.
func (t *tracker) settleRestart(rep auth.CooldownReport) []Event {
	live := make(map[windowKey]bool, len(rep.Windows))
	for _, w := range rep.Windows {
		live[windowKey{w.AuthID, w.Model}] = true
	}
	var out []Event
	for _, key := range t.sortedKeys() {
		cur := t.open[key]
		if live[key] {
			continue
		}
		if !cur.state.RetryAt.After(rep.At) {
			out = append(out, t.end(key, cur, cur.state.RetryAt, EndExpired, SourceReconcile))
		} else {
			out = append(out, t.end(key, cur, rep.At, EndRestart, SourceReconcile))
		}
	}
	return out
}

func (t *tracker) end(key windowKey, ow *openWindow, at time.Time, reason, source string) Event {
	delete(t.open, key)
	if at.Before(ow.startedAt) {
		at = ow.startedAt
	}
	ev := windowEvent(KindEnd, source, ow, at)
	ev.EndedAt = timePtr(at)
	ev.EndReason = reason
	return ev
}

func (t *tracker) sortedKeys() []windowKey {
	keys := make([]windowKey, 0, len(t.open))
	for k := range t.open {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].authID != keys[j].authID {
			return keys[i].authID < keys[j].authID
		}
		return keys[i].model < keys[j].model
	})
	return keys
}

func changed(a, b auth.CooldownWindowState) bool {
	return !a.RetryAt.Equal(b.RetryAt) || a.Reason != b.Reason || a.HTTPStatus != b.HTTPStatus ||
		a.BackoffLevel != b.BackoffLevel
}

func windowEvent(kind, source string, ow *openWindow, at time.Time) Event {
	s := ow.state
	return Event{
		EventKey:     newKey(),
		TS:           at.UTC(),
		Kind:         kind,
		Source:       source,
		CooldownID:   ow.id,
		AuthID:       s.AuthID,
		AuthIndex:    s.AuthIndex,
		AuthLabel:    s.AuthLabel,
		Provider:     s.Provider,
		Model:        s.Model,
		Reason:       truncate(s.Reason),
		HTTPStatus:   s.HTTPStatus,
		ErrorCode:    truncate(s.ErrorCode),
		Message:      truncate(s.Message),
		RetryAt:      timePtr(s.RetryAt),
		BackoffLevel: s.BackoffLevel,
		StartedAt:    timePtr(ow.startedAt),
	}
}

// applyEvent replays a stored event onto the open set (spooled events after a restart).
func (t *tracker) applyEvent(ev Event) {
	if ev.CooldownID == "" {
		return
	}
	key := windowKey{ev.AuthID, ev.Model}
	switch ev.Kind {
	case KindStart, KindUpdate:
		ow := t.open[key]
		if ow == nil || ow.id != ev.CooldownID {
			if ev.Kind == KindUpdate && ow != nil {
				return
			}
			started := ev.TS
			if ev.StartedAt != nil {
				started = *ev.StartedAt
			}
			ow = &openWindow{id: ev.CooldownID, startedAt: started}
			t.open[key] = ow
		}
		ow.state = stateOf(ev)
	case KindEnd:
		if ow := t.open[key]; ow != nil && ow.id == ev.CooldownID {
			delete(t.open, key)
		}
	}
}

func stateOf(ev Event) auth.CooldownWindowState {
	s := auth.CooldownWindowState{
		AuthID:       ev.AuthID,
		AuthIndex:    ev.AuthIndex,
		AuthLabel:    ev.AuthLabel,
		Provider:     ev.Provider,
		Model:        ev.Model,
		Reason:       ev.Reason,
		HTTPStatus:   ev.HTTPStatus,
		ErrorCode:    ev.ErrorCode,
		Message:      ev.Message,
		BackoffLevel: ev.BackoffLevel,
	}
	if ev.RetryAt != nil {
		s.RetryAt = *ev.RetryAt
	}
	return s
}
