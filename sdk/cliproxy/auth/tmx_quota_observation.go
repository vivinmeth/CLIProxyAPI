package auth

import (
	"sort"
	"strings"
	"time"
)

// QuotaObservationMaxAge bounds both passive observations and restored/polled readings.
const QuotaObservationMaxAge = 6 * time.Hour
const quotaEvidenceMaxAge = 5 * time.Minute

// SignalObservedTime preserves a window's clock across partial responses. Legacy
// snapshots without individual clocks use their original snapshot timestamp.
func (q QuotaState) SignalObservedTime(name string) time.Time {
	for key, at := range q.SignalObservedAt {
		if strings.EqualFold(key, name) {
			return at
		}
	}
	return q.ObservedAt
}

func stableQuotaSignal(name string) bool {
	lower := strings.ToLower(name)
	return strings.HasSuffix(lower, "-reset") || strings.HasSuffix(lower, "-reset-at") ||
		strings.HasSuffix(lower, "-reset-after-seconds") || strings.HasSuffix(lower, "-window-minutes") ||
		strings.HasSuffix(lower, "-utilization") || strings.HasSuffix(lower, "-used-percent") || lower == devinWeeklyResetSignal
}

// FreshSignals expires fields independently. Reset schedules and utilization may
// survive a partial response; response-specific rejection evidence does not.
func (q QuotaState) FreshSignals(now time.Time) map[string]string {
	out := make(map[string]string, len(q.Signals))
	for key, value := range q.Signals {
		at := q.SignalObservedTime(key)
		ttl := quotaEvidenceMaxAge
		if stableQuotaSignal(key) {
			ttl = QuotaObservationMaxAge
		}
		if !at.IsZero() && (now.Sub(at) > ttl || at.After(now.Add(time.Minute))) {
			continue
		}
		out[key] = value
	}
	return out
}

func (q *QuotaState) mergeObservedSignals(next map[string]string, at time.Time) {
	old := q.Clone()
	clock := at
	if old.ObservedAt.After(clock) {
		clock = old.ObservedAt
	}
	merged := old.FreshSignals(clock)
	times := make(map[string]time.Time, len(merged)+len(next))
	for key := range merged {
		// An absent rejection flag is not evidence about the new response.
		if !stableQuotaSignal(key) && !at.Before(old.SignalObservedTime(key)) {
			delete(merged, key)
			continue
		}
		times[key] = old.SignalObservedTime(key)
	}
	for key, value := range next {
		if !stableQuotaSignal(key) && at.Before(old.ObservedAt) {
			continue
		}
		if _, exists := merged[key]; exists && old.SignalObservedTime(key).After(at) {
			continue
		}
		merged[key] = value
		times[key] = at
	}
	// Keep the total bound, including partial snapshots from many namespaces.
	// collectQuotaSignals validates provider-specific fields at the caller. Here
	// retention priority is the same for all supported providers.
	if len(merged) > maxQuotaSignalHeaders {
		names := make([]string, 0, len(merged))
		for key := range merged {
			names = append(names, key)
		}
		sortQuotaSignalNames(names)
		for _, key := range names[maxQuotaSignalHeaders:] {
			delete(merged, key)
			delete(times, key)
		}
	}
	q.Signals = merged
	q.SignalObservedAt = times
	if at.After(q.ObservedAt) {
		q.ObservedAt = at
	}
}

func sortQuotaSignalNames(names []string) {
	sort.Slice(names, func(i, j int) bool {
		a, b := quotaSignalRetentionRank(names[i]), quotaSignalRetentionRank(names[j])
		if a != b {
			return a < b
		}
		return names[i] < names[j]
	})
}
