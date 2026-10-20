package resets

import (
	"encoding/json"
	"errors"
	"math"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode"
)

// Window is one quota window a reset can clear. Names follow internal/tmx/quota: "5h", "7d",
// "7d_oi" (weekly incl. overage), "30d"; anything else keeps the provider's name.
type Window struct {
	Window   string     `json:"window"`
	UsedPct  *float64   `json:"used_pct"`
	ResetsAt *time.Time `json:"resets_at"`
}

// Credit is one banked reset. Blocker is a hard reason the provider will refuse it now (the UI
// disables the button); Warning is a soft one (the spend is allowed but likely returns nothing).
type Credit struct {
	ID         string     `json:"id"`
	Title      string     `json:"title"`
	GrantedAt  *time.Time `json:"granted_at,omitempty"`
	StartsAt   *time.Time `json:"starts_at,omitempty"`
	ExpiresAt  *time.Time `json:"expires_at,omitempty"`
	Clears     []string   `json:"clears"`
	ResetsLeft int        `json:"resets_left"`
	Blocker    string     `json:"blocker,omitempty"`
	Warning    string     `json:"warning,omitempty"`
}

// Status is everything one account's provider says about its reset credits.
type Status struct {
	Eligible         bool       `json:"eligible"`
	IneligibleReason string     `json:"ineligible_reason,omitempty"`
	AtLimit          bool       `json:"at_limit"`
	CooldownUntil    *time.Time `json:"cooldown_until,omitempty"`
	ApplicableCount  *int       `json:"applicable_count,omitempty"` // Codex only
	Windows          []Window   `json:"windows"`
	Credits          []Credit   `json:"credits"`
	NextCreditID     string     `json:"next_credit_id,omitempty"`
}

var (
	errMalformed     = errors.New("malformed response")
	creditIDRe       = regexp.MustCompile(`^[A-Za-z0-9_.:-]{1,80}$`)
	claudeGrantIDRe  = regexp.MustCompile(`^[a-z0-9_-]{1,40}$`)
	claudeWindowName = map[string]string{"five_hour": "5h", "seven_day": "7d", "seven_day_overage_included": "7d_oi"}
)

func pct(f float64) *float64 {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return nil
	}
	v := math.Max(0, math.Min(100, f))
	return &v
}

func isoTime(raw any) *time.Time {
	s, ok := raw.(string)
	if !ok || strings.TrimSpace(s) == "" {
		return nil
	}
	t, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(s))
	if err != nil {
		return nil
	}
	t = t.UTC()
	return &t
}

func unixTime(raw any) *time.Time {
	f, ok := raw.(float64)
	if !ok || f <= 0 {
		return nil
	}
	t := time.Unix(int64(f), 0).UTC()
	return &t
}

// safeText strips control characters from upstream display text and bounds it.
func safeText(raw any, max int) string {
	s, _ := raw.(string)
	s = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return -1
		}
		return r
	}, s)
	s = strings.TrimSpace(s)
	if r := []rune(s); len(r) > max {
		s = string(r[:max])
	}
	return s
}

func obj(raw any) map[string]any {
	m, _ := raw.(map[string]any)
	return m
}

func sortCredits(cs []Credit) {
	sort.SliceStable(cs, func(i, j int) bool {
		a, b := cs[i].ExpiresAt, cs[j].ExpiresAt
		switch {
		case a == nil && b == nil:
			return cs[i].ID < cs[j].ID
		case a == nil:
			return false
		case b == nil:
			return true
		case !a.Equal(*b):
			return a.Before(*b)
		}
		return cs[i].ID < cs[j].ID
	})
}

// ParseClaude reads GET api.anthropic.com/api/oauth/usage?cedar_ember=1&skip_spend=1: the 5h/7d
// windows plus the cedar_ember reset-grant block. now decides not-started / expired.
func ParseClaude(body []byte, now time.Time) (Status, error) {
	var root map[string]any
	if err := json.Unmarshal(body, &root); err != nil || root == nil {
		return Status{}, errMalformed
	}
	st := Status{Windows: []Window{}, Credits: []Credit{}}
	for _, key := range []string{"five_hour", "seven_day"} {
		w := obj(root[key])
		if w == nil {
			continue
		}
		win := Window{Window: claudeWindowName[key], ResetsAt: isoTime(w["resets_at"])}
		if f, ok := w["utilization"].(float64); ok {
			win.UsedPct = pct(f)
		}
		st.Windows = append(st.Windows, win)
	}
	ce := obj(root["cedar_ember"])
	if ce == nil {
		return st, nil // account not enrolled: no credits, not an error
	}
	st.Eligible, _ = ce["eligible"].(bool)
	st.IneligibleReason = safeText(ce["ineligible_reason"], 40)
	st.AtLimit, _ = ce["at_limit"].(bool)
	st.CooldownUntil = isoTime(ce["cooldown_until"])
	next, _ := ce["next_grant_id"].(string)
	if claudeGrantIDRe.MatchString(next) {
		st.NextCreditID = next
	}
	grants, _ := ce["grants"].([]any)
	for _, raw := range grants {
		g := obj(raw)
		id, _ := g["id"].(string)
		if g == nil || !claudeGrantIDRe.MatchString(id) {
			continue
		}
		c := Credit{ID: id, Title: safeText(g["label"], 120), StartsAt: isoTime(g["starts_at"]),
			ExpiresAt: isoTime(g["ends_at"]), Clears: []string{}}
		if f, ok := g["resets_left"].(float64); ok && f > 0 {
			c.ResetsLeft = int(f)
		}
		clears, _ := g["clears"].([]any)
		for _, w := range clears {
			if name, ok := claudeWindowName[safeText(w, 40)]; ok {
				c.Clears = append(c.Clears, name)
			}
		}
		paused, _ := g["paused"].(bool)
		usable, hasUsable := g["usable_now"].(bool)
		requiresLimit, _ := g["use_requires_limit"].(bool)
		blocking, _ := g["blocking"].([]any)
		switch {
		case !st.Eligible:
			c.Blocker = "ineligible"
		case st.CooldownUntil != nil && st.CooldownUntil.After(now):
			c.Blocker = "cooldown"
		case paused:
			c.Blocker = "paused"
		case c.ResetsLeft <= 0:
			c.Blocker = "exhausted"
		case c.StartsAt != nil && c.StartsAt.After(now):
			c.Blocker = "not_started"
		case c.ExpiresAt != nil && !c.ExpiresAt.After(now):
			c.Blocker = "expired"
		case !hasUsable || !usable:
			c.Blocker = "not_usable"
		case len(blocking) > 0:
			c.Blocker = "blocked"
		case requiresLimit && !st.AtLimit:
			c.Blocker = "not_limited"
		}
		st.Credits = append(st.Credits, c)
	}
	sortCredits(st.Credits)
	return st, nil
}

func codexWindowName(seconds float64, slot string) string {
	switch {
	case seconds >= 28*86400:
		return "30d"
	case seconds >= 7*86400:
		return "7d"
	case seconds > 0 && seconds <= 5*3600:
		return "5h"
	}
	return slot
}

// ParseCodexUsage reads GET chatgpt.com/backend-api/wham/usage: the main rate-limit windows (all
// cleared by a Codex reset) and rate_limit_reset_credits.applicable_available_count.
func ParseCodexUsage(body []byte, now time.Time) (Status, error) {
	var root map[string]any
	if err := json.Unmarshal(body, &root); err != nil || root == nil {
		return Status{}, errMalformed
	}
	st := Status{Eligible: true, Windows: []Window{}, Credits: []Credit{}}
	rl := obj(root["rate_limit"])
	if limitReached, ok := rl["limit_reached"].(bool); ok {
		st.AtLimit = limitReached
	}
	for _, slot := range []string{"primary", "secondary"} {
		w := obj(rl[slot+"_window"])
		if w == nil {
			continue
		}
		secs, _ := w["limit_window_seconds"].(float64)
		win := Window{Window: codexWindowName(secs, slot), ResetsAt: unixTime(w["reset_at"])}
		if win.ResetsAt == nil {
			if after, ok := w["reset_after_seconds"].(float64); ok && after >= 0 {
				t := now.Add(time.Duration(after) * time.Second).UTC()
				win.ResetsAt = &t
			}
		}
		if f, ok := w["used_percent"].(float64); ok {
			win.UsedPct = pct(f)
		}
		st.Windows = append(st.Windows, win)
	}
	if rc := obj(root["rate_limit_reset_credits"]); rc != nil {
		if f, ok := rc["applicable_available_count"].(float64); ok && f >= 0 {
			n := int(f)
			st.ApplicableCount = &n
		}
	}
	return st, nil
}

// ParseCodexCredits reads GET chatgpt.com/backend-api/wham/rate-limit-reset-credits and fills
// st.Credits (soonest expiry first). Windows come from ParseCodexUsage.
func ParseCodexCredits(body []byte, st *Status, now time.Time) error {
	var root map[string]any
	if err := json.Unmarshal(body, &root); err != nil || root == nil {
		return errMalformed
	}
	raw, ok := root["credits"].([]any)
	if !ok {
		return errMalformed
	}
	clears := make([]string, 0, len(st.Windows))
	for _, w := range st.Windows {
		clears = append(clears, w.Window)
	}
	st.Credits = []Credit{}
	for _, item := range raw {
		c := obj(item)
		id, _ := c["id"].(string)
		status, _ := c["status"].(string)
		if c == nil || !creditIDRe.MatchString(id) || c["reset_type"] != "codex_rate_limits" ||
			(status != "available" && status != "redeeming") {
			continue
		}
		credit := Credit{ID: id, Title: safeText(c["title"], 120), GrantedAt: isoTime(c["granted_at"]),
			ExpiresAt: isoTime(c["expires_at"]), Clears: append([]string(nil), clears...), ResetsLeft: 1}
		supported, hasSupported := c["is_supported_by_plan"].(bool)
		switch {
		case status == "redeeming":
			credit.Blocker = "redeeming"
		case hasSupported && !supported:
			credit.Blocker = "unsupported_plan"
		case credit.ExpiresAt != nil && !credit.ExpiresAt.After(now):
			credit.Blocker = "expired"
		case st.ApplicableCount != nil && *st.ApplicableCount == 0:
			credit.Warning = "nothing_to_clear"
		}
		st.Credits = append(st.Credits, credit)
	}
	sortCredits(st.Credits)
	return nil
}
