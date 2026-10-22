package quota

import (
	"strconv"
	"time"

	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

// The manager retains partial windows with separate clocks. Never turn a new
// 5h header into a new weekly SQL reading (or move a relative Codex reset).
func passiveReadings(auth *coreauth.Auth, spec providerSpec, now time.Time) []Current {
	signals := auth.Quota.FreshSignals(now)
	if auth.Provider == "codex" {
		for _, slot := range []string{"Primary", "Secondary"} {
			base := "X-Codex-" + slot + "-"
			if signals[base+"Reset-At"] != "" {
				continue
			}
			seconds, err := strconv.ParseInt(signals[base+"Reset-After-Seconds"], 10, 64)
			if err == nil && seconds >= 0 {
				signals[base+"Reset-At"] = strconv.FormatInt(auth.Quota.SignalObservedTime(base+"Reset-After-Seconds").Add(time.Duration(seconds)*time.Second).Unix(), 10)
			}
		}
	}
	windows := spec.passive(signals, auth.Quota.ObservedAt)
	readings := make([]Current, 0, len(windows))
	for _, w := range windows {
		prefix := "Anthropic-Ratelimit-Unified-" + w.Window + "-"
		fields := []string{prefix + "Utilization", prefix + "Reset", prefix + "Status"}
		if auth.Provider == "meta" {
			fields = metaSignalFields(signals, w.Window)
		}
		if auth.Provider == "codex" {
			fields = nil
			for _, slot := range []string{"Primary", "Secondary"} {
				base := "X-Codex-" + slot + "-"
				minutes, _ := strconv.ParseInt(signals[base+"Window-Minutes"], 10, 64)
				if windowNameForSeconds(minutes*60) == w.Window {
					fields = append(fields, base+"Used-Percent", base+"Reset-After-Seconds", base+"Reset-At")
				}
			}
		}
		var at time.Time
		for _, key := range fields {
			if _, ok := auth.Quota.Signals[key]; ok {
				if t := auth.Quota.SignalObservedTime(key); t.After(at) {
					at = t
				}
			}
		}
		if at.IsZero() {
			at = auth.Quota.ObservedAt
		}
		readings = append(readings, Current{Window: w, ObservedAt: at, Source: "passive"})
	}
	return readings
}
