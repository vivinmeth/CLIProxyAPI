package auth

import "strings"

// tmxDropImageLimitWindows keeps a Codex image response from posing as the account's coding
// quota. ChatGPT's images endpoint answers with X-Codex-Active-Limit: imagegen_premium and puts the
// image limit (a daily window on prolite, 2026-11) in the same X-Codex-Primary-*/Secondary-* slots
// coding responses use for their 5h/weekly windows. Stored at credential level, that reading would
// replace the weekly window reset-soonest ranks on and feed the image usage into the coding
// utilization threshold (overQuotaThreshold) until the next coding response. So for an image
// limit only the window fields are dropped; plan and credits are account-wide and stay. The image
// window is not recorded passively; if wham/usage lists it, polling names it "<limit>:<window>"
// (ParseCodexUsage).
func tmxDropImageLimitWindows(provider string, signals map[string]string) map[string]string {
	if !strings.EqualFold(strings.TrimSpace(provider), "codex") || len(signals) == 0 {
		return signals
	}
	if !strings.HasPrefix(strings.ToLower(quotaSignalValue(signals, "X-Codex-Active-Limit")), "imagegen") {
		return signals
	}
	kept := make(map[string]string, len(signals))
	for name, value := range signals {
		lower := strings.ToLower(name)
		if lower == "x-codex-plan-type" || strings.HasPrefix(lower, "x-codex-credits-") {
			kept[name] = value
		}
	}
	return kept
}
