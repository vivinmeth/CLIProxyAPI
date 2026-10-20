package quota

import (
	"encoding/json"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Window is one quota window of one account at one moment.
//
// Window names are canonical across sources so passive and polled readings line up:
//
//	5h           rolling five-hour session window
//	7d           weekly window shared by all models
//	7d:<model>   weekly window scoped to one model ("7d:fable-5"), from the usage API
//	7d_<name>    other named weekly buckets from the usage API ("7d_opus", "7d_sonnet", ...)
type Window struct {
	Window   string     `json:"window"`
	UsedPct  *float64   `json:"used_pct"` // 0-100; nil when the source gives none
	ResetsAt *time.Time `json:"resets_at"`
	Status   string     `json:"status,omitempty"` // passive only: allowed / allowed_warning / rejected
}

// claudeHeaderWindows maps the passive unified rate-limit header prefixes to canonical windows.
var claudeHeaderWindows = map[string]string{"5h": "5h", "7d": "7d", "7d_oi": "7d_oi"}

// ParseClaudeSignals reads the anthropic-ratelimit-unified-* headers the proxy keeps per account
// (utilization is a 0-1 fraction, reset a unix timestamp).
func ParseClaudeSignals(signals map[string]string) []Window {
	get := func(name string) string {
		for k, v := range signals {
			if strings.EqualFold(k, name) {
				return strings.TrimSpace(v)
			}
		}
		return ""
	}
	var out []Window
	for prefix, name := range claudeHeaderWindows {
		base := "Anthropic-Ratelimit-Unified-" + prefix + "-"
		util, reset, status := get(base+"Utilization"), get(base+"Reset"), get(base+"Status")
		if util == "" && reset == "" && status == "" {
			continue
		}
		w := Window{Window: name, Status: status}
		if f, err := strconv.ParseFloat(util, 64); err == nil {
			w.UsedPct = pct(f * 100)
		}
		if n, err := strconv.ParseInt(reset, 10, 64); err == nil && n > 0 {
			t := time.Unix(n, 0).UTC()
			w.ResetsAt = &t
		}
		out = append(out, w)
	}
	sortWindows(out)
	return out
}

type claudeUsageWindow struct {
	Utilization  *float64 `json:"utilization"`
	ResetsAt     string   `json:"resets_at"`
	LimitDollars *float64 `json:"limit_dollars"`
}

type claudeLimit struct {
	Kind     string   `json:"kind"`
	Percent  *float64 `json:"percent"`
	ResetsAt string   `json:"resets_at"`
	Scope    *struct {
		Model *struct {
			DisplayName string `json:"display_name"`
		} `json:"model"`
	} `json:"scope"`
}

// claudeNamedWindows are the usage-API keys we record. Unknown codenamed keys are skipped until we
// know what they mean; the raw payload is not stored.
var claudeNamedWindows = map[string]string{
	"five_hour":            "5h",
	"seven_day":            "7d",
	"seven_day_opus":       "7d_opus",
	"seven_day_sonnet":     "7d_sonnet",
	"seven_day_oauth_apps": "7d_oauth_apps",
	"seven_day_cowork":     "7d_cowork",
}

var nonSlug = regexp.MustCompile(`[^a-z0-9]+`)

// ParseClaudeUsage reads GET api.anthropic.com/api/oauth/usage. Utilization there is already 0-100.
// Model-scoped weekly limits ("Fable 5") come from limits[kind=weekly_scoped].
func ParseClaudeUsage(body []byte) ([]Window, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, err
	}
	var out []Window
	for key, name := range claudeNamedWindows {
		var w claudeUsageWindow
		if len(raw[key]) == 0 || json.Unmarshal(raw[key], &w) != nil || w.Utilization == nil || w.LimitDollars != nil {
			continue
		}
		out = append(out, Window{Window: name, UsedPct: pct(*w.Utilization), ResetsAt: parseTime(w.ResetsAt)})
	}
	var limits []claudeLimit
	if len(raw["limits"]) > 0 {
		_ = json.Unmarshal(raw["limits"], &limits)
	}
	seen := map[string]bool{}
	for _, l := range limits {
		if l.Kind != "weekly_scoped" || l.Percent == nil || l.Scope == nil || l.Scope.Model == nil {
			continue
		}
		slug := strings.Trim(nonSlug.ReplaceAllString(strings.ToLower(l.Scope.Model.DisplayName), "-"), "-")
		if slug == "" || seen[slug] {
			continue
		}
		seen[slug] = true
		out = append(out, Window{Window: "7d:" + slug, UsedPct: pct(*l.Percent), ResetsAt: parseTime(l.ResetsAt)})
	}
	sortWindows(out)
	return out, nil
}

// ParseClaudePlan reads GET api.anthropic.com/api/oauth/profile into a short plan label:
// "max_20x", "max_5x", "pro", "team", "free", or "" when unknown.
func ParseClaudePlan(body []byte) string {
	var p struct {
		Account struct {
			HasClaudeMax *bool `json:"has_claude_max"`
			HasClaudePro *bool `json:"has_claude_pro"`
		} `json:"account"`
		Organization struct {
			Type          string `json:"organization_type"`
			RateLimitTier string `json:"rate_limit_tier"`
			Subscription  string `json:"subscription_status"`
		} `json:"organization"`
	}
	if json.Unmarshal(body, &p) != nil {
		return ""
	}
	tier := strings.ToLower(p.Organization.RateLimitTier)
	switch {
	case p.Organization.Type == "claude_team" && p.Organization.Subscription == "active":
		return "team"
	case p.Account.HasClaudeMax != nil && *p.Account.HasClaudeMax:
		if strings.Contains(tier, "20x") {
			return "max_20x"
		}
		if strings.Contains(tier, "5x") {
			return "max_5x"
		}
		return "max"
	case p.Account.HasClaudePro != nil && *p.Account.HasClaudePro:
		return "pro"
	case p.Account.HasClaudeMax != nil && p.Account.HasClaudePro != nil:
		return "free"
	}
	return ""
}

func parseTime(s string) *time.Time {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return nil
	}
	t = t.UTC().Truncate(time.Second)
	return &t
}

func pct(v float64) *float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return nil
	}
	v = math.Round(v*100) / 100
	return &v
}

func sortWindows(ws []Window) {
	for i := 1; i < len(ws); i++ {
		for j := i; j > 0 && ws[j].Window < ws[j-1].Window; j-- {
			ws[j], ws[j-1] = ws[j-1], ws[j]
		}
	}
}
