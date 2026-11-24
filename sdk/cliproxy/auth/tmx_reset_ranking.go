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
		keys := append(claudeWeeklyResetNames(a.Quota.Signals, model), devinWeeklyResetSignal, tmxQuotaWeeklyResetSignal)
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
	over := false
	eachOverQuotaWindow(a, now, model, false, v, func(string, string) bool {
		over = true
		return false
	})
	return over
}

// eachOverQuotaWindow calls over(window, field) for each window routing holds at or over its
// threshold for model, or for every model when allModels is set. window is the routing name ("5h",
// "7d", "7d_oi", ...); field is the signal whose clock dates the reading. It stops when over
// returns false.
func eachOverQuotaWindow(a *Auth, now time.Time, model string, allModels bool, v tmxAdmissionView, over func(window, field string) bool) {
	if a == nil {
		return
	}
	// A reading past its TTL still counts while its own window hasn't reset: utilization only
	// grows within a window, so a last-known exhausted account stays behind those with headroom
	// (it remains a last-resort probe) until a fresh reading replaces it.
	signals := a.Quota.FreshSignals(now)
	known := a.Quota.Signals
	type window struct {
		name, used, reset, after string
		scale, threshold         float64
	}
	windows := []window{
		{"5h", "Anthropic-Ratelimit-Unified-5h-Utilization", "Anthropic-Ratelimit-Unified-5h-Reset", "", 1, v.five},
		{"7d", "Anthropic-Ratelimit-Unified-7d-Utilization", claudeWeeklyResetSignal, "", 1, v.seven},
	}
	for _, name := range claudeWeeklyResetNamesFor(known, model, allModels) {
		if name == claudeWeeklyResetSignal {
			continue
		}
		windows = append(windows, window{claudeWindowName(name), strings.TrimSuffix(name, "Reset") + "Utilization", name, "", 1, v.seven})
	}
	for _, slot := range []string{"Primary", "Secondary"} {
		minutes, ok := parseQuotaInt(quotaSignalValue(known, "X-Codex-"+slot+"-Window-Minutes"))
		if !ok {
			continue
		}
		name, threshold := "5h", v.five
		if minutes >= minimumWeeklyWindowMinutes {
			name, threshold = "7d", v.seven
		} else if minutes > 24*60 {
			continue
		}
		base := "X-Codex-" + slot + "-"
		windows = append(windows, window{name, base + "Used-Percent", base + "Reset-At", base + "Reset-After-Seconds", 100, threshold})
	}
	for _, name := range tmxQuotaWindows {
		threshold := v.seven
		if name == "5h" {
			threshold = v.five
		}
		base := TmxQuotaSignalPrefix + name + "-"
		windows = append(windows, window{name, base + "Used-Percent", base + "Reset", "", 100, threshold})
		// The provider itself says the window is spent (OpenCode Go "rate-limited"): last resort
		// until that window resets, however low its rounded percent reads.
		if strings.EqualFold(quotaSignalValue(known, base+"Status"), "rejected") {
			if reset, ok := parseQuotaResetTimestamp(quotaSignalValue(known, base+"Reset")); ok && reset.After(now) {
				if !over(name, base+"Status") {
					return
				}
				windows = windows[:len(windows)-1]
			}
		}
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
		value := quotaSignalValue(signals, w.used)
		if value == "" && hasReset {
			value = quotaSignalValue(known, w.used)
		}
		used, err := strconv.ParseFloat(value, 64)
		if err == nil && used/w.scale >= w.threshold && !over(w.name, w.used) {
			return
		}
	}
}

// claudeWindowName is the routing window of a Claude reset signal ("7d_oi" from
// "Anthropic-Ratelimit-Unified-7d_oi-Reset").
func claudeWindowName(resetSignal string) string {
	const prefix, suffix = len("Anthropic-Ratelimit-Unified-"), len("-Reset")
	if len(resetSignal) <= prefix+suffix {
		return resetSignal
	}
	return resetSignal[prefix : len(resetSignal)-suffix]
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
	return claudeWeeklyResetNamesFor(signals, model, false)
}

// claudeWeeklyResetNamesFor is the Claude weekly reset signals routing weighs for model. With
// anyModel it is their union over every model routing can rank: each model-scoped rule applies,
// and windows no rule names (7d_oauth_apps, 7d_cowork) stay out, as they do for every model.
func claudeWeeklyResetNamesFor(signals map[string]string, model string, anyModel bool) []string {
	names := []string{claudeWeeklyResetSignal}
	lowerModel := strings.ToLower(model)
	applies := func(scope string) bool { return anyModel || strings.Contains(lowerModel, scope) }
	if applies("fable") {
		names = append(names, claudeWeeklyModelResetSignal)
	}
	for _, family := range []string{"opus", "sonnet"} {
		if applies(family) {
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
		if slug != "" && applies(slug) {
			names = append(names, key)
		}
	}
	return names
}
