package auth

import (
	"strconv"
	"strings"
	"time"
)

type tmxRankingModelKey struct{}

func codexWeeklyWindow(signals map[string]string, slot string) bool {
	minutes, ok := parseQuotaInt(quotaSignalValue(signals, "X-Codex-"+slot+"-Window-Minutes"))
	return ok && minutes >= minimumWeeklyWindowMinutes
}

func rankingKey(a *Auth, now time.Time, model string) (int, time.Time) {
	reset, ok := weeklyResetAtForModel(a, now, model)
	if ok {
		return resetSoonestRankKnown, reset
	}
	// A reset that rolled over isn't a never-observed login deserving a probe.
	if a != nil {
		keys := append(claudeWeeklyResetNames(a.Quota.Signals, model), devinWeeklyResetSignal)
		for _, slot := range []string{"Primary", "Secondary"} {
			if codexWeeklyWindow(a.Quota.Signals, slot) {
				keys = append(keys, "X-Codex-"+slot+"-Reset-At")
			}
		}
		for _, key := range keys {
			if t, valid := parseQuotaResetTimestamp(quotaSignalValue(a.Quota.Signals, key)); valid && !t.After(now) {
				return resetSoonestRankPast, time.Time{}
			}
		}
	}
	return resetSoonestRankUnknown, time.Time{}
}

func overQuotaThreshold(a *Auth, now time.Time, model string, v tmxAdmissionView) bool {
	if a == nil {
		return false
	}
	signals := a.Quota.FreshSignals(now)
	type window struct {
		used, reset, after string
		scale, threshold   float64
	}
	windows := []window{
		{"Anthropic-Ratelimit-Unified-5h-Utilization", "Anthropic-Ratelimit-Unified-5h-Reset", "", 1, v.five},
		{"Anthropic-Ratelimit-Unified-7d-Utilization", claudeWeeklyResetSignal, "", 1, v.seven},
	}
	for _, name := range claudeWeeklyResetNames(signals, model) {
		if name == claudeWeeklyResetSignal {
			continue
		}
		windows = append(windows, window{strings.TrimSuffix(name, "Reset") + "Utilization", name, "", 1, v.seven})
	}
	for _, slot := range []string{"Primary", "Secondary"} {
		minutes, ok := parseQuotaInt(quotaSignalValue(signals, "X-Codex-"+slot+"-Window-Minutes"))
		if !ok {
			continue
		}
		threshold := v.five
		if minutes >= minimumWeeklyWindowMinutes {
			threshold = v.seven
		} else if minutes > 24*60 {
			continue
		}
		base := "X-Codex-" + slot + "-"
		windows = append(windows, window{base + "Used-Percent", base + "Reset-At", base + "Reset-After-Seconds", 100, threshold})
	}
	for _, w := range windows {
		// Utilization belongs to its observed reset window, and stops applying at rollover.
		reset, hasReset := parseQuotaResetTimestamp(quotaSignalValue(a.Quota.Signals, w.reset))
		if !hasReset && w.after != "" {
			reset, hasReset = codexWindowResetAt(a.Quota.Signals, a.Quota.SignalObservedTime(w.after), w.reset, w.after)
		}
		if hasReset && !reset.After(now) {
			continue
		}
		used, err := strconv.ParseFloat(quotaSignalValue(signals, w.used), 64)
		if err == nil && used/w.scale >= w.threshold {
			return true
		}
	}
	return false
}

// Keep reset-soonest's burn order within headroom classes. Saturated/unknown
// probes spill before exhausted accounts, which remain available as a last resort.
func pickResetSoonestForModel(auths []*Auth, now time.Time, model string, v tmxAdmissionView) *Auth {
	var best *Auth
	var bestPenalty, bestRank int
	var bestCount int64
	var bestReset time.Time
	for _, a := range auths {
		if a == nil {
			continue
		}
		rank, reset := rankingKey(a, now, model)
		count := v.count(a.ID)
		penalty := 0
		if (rank == resetSoonestRankUnknown && count >= 1) || (v.cap > 0 && count >= int64(v.cap)) {
			penalty = 1
		}
		if overQuotaThreshold(a, now, model, v) {
			penalty = 2
		}
		better := best == nil || penalty < bestPenalty
		if best != nil && penalty == bestPenalty {
			// Once every candidate is saturated, spread excess load evenly (soft cap).
			if penalty == 1 && count != bestCount {
				better = count < bestCount
			} else {
				better = rank < bestRank || (rank == bestRank && rank == resetSoonestRankKnown && reset.Before(bestReset))
			}
		}
		if better {
			best, bestPenalty, bestRank, bestCount, bestReset = a, penalty, rank, count, reset
		}
	}
	return best
}

func claudeWeeklyResetNames(signals map[string]string, model string) []string {
	names := []string{claudeWeeklyResetSignal}
	lowerModel := strings.ToLower(model)
	if strings.Contains(lowerModel, "fable") {
		names = append(names, claudeWeeklyModelResetSignal)
	}
	for _, family := range []string{"opus", "sonnet"} {
		if strings.Contains(lowerModel, family) {
			names = append(names, "Anthropic-Ratelimit-Unified-7d_"+family+"-Reset")
		}
	}
	const prefix = "anthropic-ratelimit-unified-7d_model_"
	for key := range signals {
		lower := strings.ToLower(key)
		if !strings.HasPrefix(lower, prefix) || !strings.HasSuffix(lower, "-reset") {
			continue
		}
		slug := strings.TrimSuffix(strings.TrimPrefix(lower, prefix), "-reset")
		if slug != "" && strings.Contains(lowerModel, slug) {
			names = append(names, key)
		}
	}
	return names
}
