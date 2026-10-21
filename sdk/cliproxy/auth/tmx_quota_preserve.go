package auth

// tokenmaxx fork: an auth update from the watcher (or any replace-style Update) carries the auth
// file, not runtime quota state. Replacing Auth.Quota with it dropped the usage-API readings the
// quota poller and the boot seed had just applied: at startup the watcher bootstrap re-syncs
// every auth right after the first poll, so a 97% account ranked first until passive headers
// arrived (load test B3). Observations and reset-clear watermarks now carry over for the same
// account, merged per field clock.

import (
	"strings"
	"time"
)

// preserveQuotaObservations merges existing's quota observations and clear watermarks into
// incoming, unless incoming is a different account.
func preserveQuotaObservations(existing, incoming *Auth) {
	if existing == nil || incoming == nil || quotaAccountChanged(existing, incoming) {
		return
	}
	q := incoming.Quota.Clone()
	for mark, at := range existing.Quota.SignalClearedAt {
		if q.SignalClearedAt == nil {
			q.SignalClearedAt = make(map[string]time.Time, len(existing.Quota.SignalClearedAt))
		}
		if at.After(q.SignalClearedAt[mark]) {
			q.SignalClearedAt[mark] = at
		}
	}
	incoming.Quota = mergeQuotaObservation(q, existing.Quota)
}

// quotaAccountChanged reports whether incoming names another provider account than existing.
// An identity field missing on either side is not evidence of a change.
func quotaAccountChanged(existing, incoming *Auth) bool {
	if !strings.EqualFold(strings.TrimSpace(existing.Provider), strings.TrimSpace(incoming.Provider)) {
		return true
	}
	for _, key := range []string{"account_uuid", "account_id", "email"} {
		before, after := authMetadataString(existing, key), authMetadataString(incoming, key)
		if before != "" && after != "" && !strings.EqualFold(before, after) {
			return true
		}
	}
	return false
}
