package quota

import (
	"strconv"
	"strings"
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
		readings = append(readings, Current{Window: w, ObservedAt: WindowObservedAt(auth.Quota, auth.Provider, w.Window, signals), Source: "passive"})
	}
	return readings
}

// WindowObservedAt is one passive window's own clock: the newest clock of the retained signal
// fields behind it, or the account-wide ObservedAt when none is retained. A window that no
// response has refreshed keeps its age even while other windows (a new 5h header) move on.
// signals names the fields: Codex and Meta windows are found by the window length they carry.
func WindowObservedAt(q coreauth.QuotaState, provider, window string, signals map[string]string) time.Time {
	var fields []string
	switch {
	case provider == "meta":
		fields = metaSignalFields(signals, window)
	case provider == "codex":
		for _, slot := range []string{"Primary", "Secondary"} {
			base := "X-Codex-" + slot + "-"
			minutes, _ := strconv.ParseInt(signals[base+"Window-Minutes"], 10, 64)
			if windowNameForSeconds(minutes*60) == window {
				fields = append(fields, base+"Used-Percent", base+"Reset-After-Seconds", base+"Reset-At")
			}
		}
	case strings.HasPrefix(provider, "openai-compatible-"):
		// OpenCode Go: the polled windows routing keeps on the compat credential.
		base := coreauth.TmxQuotaSignalPrefix + window + "-"
		fields = []string{base + "Used-Percent", base + "Reset", base + "Status"}
	default:
		prefix := "Anthropic-Ratelimit-Unified-" + window + "-"
		fields = []string{prefix + "Utilization", prefix + "Reset", prefix + "Status"}
	}
	var at time.Time
	for _, key := range fields {
		if _, ok := q.Signals[key]; ok {
			if t := q.SignalObservedTime(key); t.After(at) {
				at = t
			}
		}
	}
	if at.IsZero() {
		at = q.ObservedAt
	}
	return at
}
