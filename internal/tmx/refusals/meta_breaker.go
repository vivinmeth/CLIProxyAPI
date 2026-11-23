package refusals

import "time"

const MetaBreakerAlert = "meta_refusal_breaker_tripped: automatic Meta disables paused; later attestation refusals cool down; inspect the Muse client before manually re-enabling accounts"

// MetaBreaker is process-local. Its owner must serialize all access with the
// same lock used to apply account disables. No account or credential is exposed.
type MetaBreaker struct {
	disabled    map[string]time.Time
	lastRefusal time.Time
	tripped     bool
}

type MetaBreakerStatus struct {
	Tripped          bool       `json:"tripped"`
	Threshold        int        `json:"threshold"`
	WindowSeconds    int        `json:"window_seconds"`
	DisablesInWindow int        `json:"disables_in_window"`
	QuietUntil       *time.Time `json:"quiet_until,omitempty"`
	Alert            string     `json:"alert,omitempty"`
}

func (b *MetaBreaker) expire(now time.Time, window time.Duration) {
	if !b.lastRefusal.IsZero() && !now.Before(b.lastRefusal.Add(window)) {
		*b = MetaBreaker{}
		return
	}
	for id, at := range b.disabled {
		if !now.Before(at.Add(window)) {
			delete(b.disabled, id)
		}
	}
}

// Allow records a current, enabled Meta account's terminal refusal before the
// disable transition. A failed persistence write still disables it in memory,
// so that transition consumes a slot too. Suppressed refusals extend quiet time.
// The second return value requests exactly one alert on a new trip.
func (b *MetaBreaker) Allow(id string, now time.Time, threshold int, window time.Duration) (bool, bool) {
	b.expire(now, window)
	b.lastRefusal = now
	if b.tripped {
		return false, false
	}
	if _, duplicate := b.disabled[id]; duplicate {
		return false, false
	}
	if len(b.disabled) >= threshold {
		b.tripped = true
		return false, true
	}
	if b.disabled == nil {
		b.disabled = make(map[string]time.Time)
	}
	b.disabled[id] = now
	if len(b.disabled) >= threshold {
		b.tripped = true
		return true, true
	}
	return true, false
}

func (b *MetaBreaker) Status(now time.Time, threshold int, window time.Duration) MetaBreakerStatus {
	b.expire(now, window)
	out := MetaBreakerStatus{Tripped: b.tripped, Threshold: threshold, WindowSeconds: int(window / time.Second), DisablesInWindow: len(b.disabled)}
	if b.tripped {
		until := b.lastRefusal.Add(window).UTC()
		out.QuietUntil, out.Alert = &until, MetaBreakerAlert
	}
	return out
}
