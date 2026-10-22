package quota

import (
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/muse"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

// Meta Muse Code subscriptions report usage in two places:
//   - passive: response.subscription_usage events on every /v1/responses stream, kept per
//     credential as X-Meta-Subscription-* signals (internal/tmx/muse);
//   - polled: POST <api origin>/muse-code/key with the login's DCA token, the call the management
//     UI's Meta card makes. It mints a fresh LLM key (thrown away here) and answers subs_usage plus
//     subs_tier_name. It spends no subscription quota. TMX_QUOTA_META_POLL=off keeps Meta passive.
//
// Both carry a rolling window (window_duration_mins, 300 today) and a weekly one.

const (
	metaKeyPath     = "/muse-code/key"
	metaWeeklyMins  = 10080
	metaHeaderBase  = muse.SubscriptionHeaderPrefix
	metaSlotWindow  = "Window"
	metaSlotWeekly  = "Weekly"
	metaDefaultHost = "https://api.meta.ai"
)

var metaSpec = providerSpec{
	usageURLFor: metaKeyURL,
	method:      "POST",
	body:        []byte("{}"),
	canPoll:     metaCanPoll,
	headers: func(auth *coreauth.Auth) map[string]string {
		return map[string]string{
			"Authorization": "Bearer " + metaDCAToken(auth),
			"Content-Type":  "application/json",
			"Accept":        "application/json",
			"User-Agent":    "muse-code/1.0.2",
			"x-api-version": "1.0.0",
		}
	},
	passive:    ParseMetaSignals,
	parseUsage: ParseMetaKeyUsage,
}

// metaWindowName names a Meta window by its length: 5h, 7d, otherwise <minutes>m.
func metaWindowName(mins int64) string {
	switch mins {
	case 300:
		return "5h"
	case metaWeeklyMins:
		return "7d"
	}
	if mins <= 0 {
		return ""
	}
	return strconv.FormatInt(mins, 10) + "m"
}

// metaSlotMinutes is a slot's window length; the weekly slot carries none and is 7 days.
func metaSlotMinutes(slot string, raw int64) int64 {
	if raw > 0 {
		return raw
	}
	if slot == metaSlotWeekly {
		return metaWeeklyMins
	}
	return 0
}

// ParseMetaSignals reads the X-Meta-Subscription-* signals the proxy keeps per Meta credential.
func ParseMetaSignals(signals map[string]string, _ time.Time) []Window {
	get := func(name string) string {
		for k, v := range signals {
			if strings.EqualFold(k, name) {
				return strings.TrimSpace(v)
			}
		}
		return ""
	}
	var out []Window
	for _, slot := range []string{metaSlotWindow, metaSlotWeekly} {
		base := metaHeaderBase + slot + "-"
		used, errUsed := strconv.ParseFloat(get(base+"Used-Percent"), 64)
		reset, _ := strconv.ParseInt(get(base+"Reset-At"), 10, 64)
		if errUsed != nil && reset <= 0 {
			continue
		}
		mins, _ := strconv.ParseInt(get(base+"Window-Minutes"), 10, 64)
		name := metaWindowName(metaSlotMinutes(slot, mins))
		if name == "" {
			continue
		}
		w := Window{Window: name}
		if errUsed == nil {
			w.UsedPct = pct(used)
		}
		if reset > 0 {
			t := time.Unix(reset, 0).UTC()
			w.ResetsAt = &t
		}
		out = append(out, w)
	}
	sortWindows(out)
	return out
}

// metaSignalFields lists the signal keys behind one parsed passive window, for its observed time.
func metaSignalFields(signals map[string]string, window string) []string {
	var fields []string
	for _, slot := range []string{metaSlotWindow, metaSlotWeekly} {
		base := metaHeaderBase + slot + "-"
		mins, _ := strconv.ParseInt(signals[base+"Window-Minutes"], 10, 64)
		if metaWindowName(metaSlotMinutes(slot, mins)) == window {
			fields = append(fields, base+"Used-Percent", base+"Reset-At")
		}
	}
	return fields
}

type metaUsageWindow struct {
	UsedPercent *float64 `json:"used_percent"`
	ResetsAt    int64    `json:"resets_at"`
	Minutes     int64    `json:"window_duration_mins"`
}

// ParseMetaKeyUsage reads the /muse-code/key answer: subs_usage.{window,weekly} and the plan from
// subs_tier_name. The minted api_key in the same payload is never read.
func ParseMetaKeyUsage(body []byte) ([]Window, string, error) {
	var p struct {
		TierName string `json:"subs_tier_name"`
		Usage    *struct {
			Window *metaUsageWindow `json:"window"`
			Weekly *metaUsageWindow `json:"weekly"`
		} `json:"subs_usage"`
	}
	if err := json.Unmarshal(body, &p); err != nil {
		return nil, "", err
	}
	var out []Window
	if p.Usage != nil {
		for _, s := range []struct {
			slot string
			w    *metaUsageWindow
		}{{metaSlotWindow, p.Usage.Window}, {metaSlotWeekly, p.Usage.Weekly}} {
			if s.w == nil || s.w.UsedPercent == nil {
				continue
			}
			name := metaWindowName(metaSlotMinutes(s.slot, s.w.Minutes))
			if name == "" {
				continue
			}
			w := Window{Window: name, UsedPct: pct(*s.w.UsedPercent)}
			if s.w.ResetsAt > 0 {
				t := time.Unix(s.w.ResetsAt, 0).UTC()
				w.ResetsAt = &t
			}
			out = append(out, w)
		}
	}
	sortWindows(out)
	return out, MetaPlan(p.TierName), nil
}

// MetaPlan maps Meta's tier name ("Everyday Usage", "High Usage", "Power Usage", "Maximum Usage",
// per dev.meta.ai/docs/muse-code/subscriptions; the key answer may prefix "Muse Code") to one
// lowercase word, or "" for anything else.
func MetaPlan(tierName string) string {
	name := strings.ToLower(strings.Join(strings.Fields(tierName), " "))
	for _, plan := range []string{"everyday", "high", "power", "maximum"} {
		if name == plan+" usage" || strings.HasSuffix(name, " "+plan+" usage") {
			return plan
		}
	}
	return ""
}

// metaKeyURL is /muse-code/key at the origin of the credential's API base URL, like the catalog.
func metaKeyURL(auth *coreauth.Auth) string {
	base := metaDefaultHost
	if auth != nil {
		if b := strings.TrimSpace(auth.Attributes["base_url"]); b != "" {
			base = b
		} else if b, ok := auth.Metadata["base_url"].(string); ok && strings.TrimSpace(b) != "" {
			base = strings.TrimSpace(b)
		}
	}
	if scheme, rest, ok := strings.Cut(base, "://"); ok {
		host, _, _ := strings.Cut(rest, "/")
		if host != "" {
			return scheme + "://" + host + metaKeyPath
		}
	}
	return metaDefaultHost + metaKeyPath
}

func metaDCAToken(auth *coreauth.Auth) string {
	if auth == nil {
		return ""
	}
	if d := strings.TrimSpace(auth.Attributes["dca_token"]); strings.HasPrefix(d, "dca:") {
		return d
	}
	if d, ok := auth.Metadata["dca_token"].(string); ok && strings.HasPrefix(strings.TrimSpace(d), "dca:") {
		return strings.TrimSpace(d)
	}
	return ""
}

// metaCanPoll: only logins with a DCA token can read /muse-code/key.
func metaCanPoll(auth *coreauth.Auth) bool {
	if strings.EqualFold(strings.TrimSpace(os.Getenv("TMX_QUOTA_META_POLL")), "off") {
		return false
	}
	return metaDCAToken(auth) != ""
}

// metaStoredPlan is the plan the login saved from its own key mint, until a poll reads a fresh one.
func metaStoredPlan(auth *coreauth.Auth) string {
	if auth == nil || auth.Metadata == nil {
		return ""
	}
	name, _ := auth.Metadata["subs_tier_name"].(string)
	return MetaPlan(name)
}
