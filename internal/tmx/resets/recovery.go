package resets

import (
	"slices"

	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

// claudeModelWindow is the only Claude window that limits one model (Fable) rather than the
// account; recovering it releases model-scoped quota cooldowns.
const claudeModelWindow = "7d_oi"

// recoveryOf decides what a settled redeem lets the proxy release, from the readings before and
// after it and the credit's scope (clears; empty means every window, as for claims journaled
// before the scope was recorded).
//
// With an after reading (afterRead):
//   - a window in scope recovered when it now reads below 100% and either (nearly) empty or
//     clearly lower than before; its passive signals are dropped;
//   - a window in scope that the usage read does not report (Claude's 7d_oi) counts as recovered
//     only when every readable window in scope recovered;
//   - the credential quota is released only when no window reads exhausted any more;
//   - Claude's model-scoped quota (Fable) is released when 7d_oi recovered.
//
// Without one, only the scoped quota cooldown is released: the credential quota when every window
// that was exhausted before is in scope, the model quota when 7d_oi is in scope. Signals stay.
func recoveryOf(provider string, before, after []Window, afterRead bool, clears []string) coreauth.QuotaRecovery {
	scope := clears
	if len(scope) == 0 {
		scope = allWindows(provider, before, after)
	}
	exhausted := func(w Window) bool { return w.UsedPct != nil && *w.UsedPct >= 100 }
	var rec coreauth.QuotaRecovery

	if !afterRead {
		rec.CredentialQuota = true
		for _, w := range before {
			if exhausted(w) && !slices.Contains(scope, w.Window) {
				rec.CredentialQuota = false
			}
		}
		if provider == "claude" && slices.Contains(scope, claudeModelWindow) {
			rec.ModelWindows = []string{claudeModelWindow}
		}
		return rec
	}

	prev := map[string]*float64{}
	for _, w := range before {
		prev[w.Window] = w.UsedPct
	}
	read := map[string]bool{}
	allRecovered := true
	for _, w := range after {
		read[w.Window] = true
		if !slices.Contains(scope, w.Window) || w.UsedPct == nil {
			continue
		}
		p := prev[w.Window]
		if *w.UsedPct < 100 && (*w.UsedPct <= 1 || (p != nil && *w.UsedPct < *p-0.5)) {
			rec.Windows = append(rec.Windows, w.Window)
		} else {
			allRecovered = false
		}
	}
	if allRecovered {
		for _, w := range scope {
			if !read[w] {
				rec.Windows = append(rec.Windows, w)
			}
		}
	}
	rec.CredentialQuota = true
	for _, w := range after {
		if exhausted(w) {
			rec.CredentialQuota = false
		}
	}
	if provider == "claude" && slices.Contains(rec.Windows, claudeModelWindow) {
		rec.ModelWindows = []string{claudeModelWindow}
	}
	return rec
}

// allWindows is the scope of a credit whose clears list is unknown: every Claude window, or every
// Codex window read (a Codex credit clears all of them).
func allWindows(provider string, before, after []Window) []string {
	if provider == "claude" {
		return []string{"5h", "7d", claudeModelWindow}
	}
	var out []string
	for _, w := range append(append([]Window(nil), before...), after...) {
		if !slices.Contains(out, w.Window) {
			out = append(out, w.Window)
		}
	}
	return out
}
