package quota

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/opencode"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/requestlog"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	log "github.com/sirupsen/logrus"
)

// OpenCode Go readings: one quota account per OpenCode account, id opencode:<entry>:<account>
// (opencode:<entry> without accounts; provider = the entry, label "OpenCode Go · <account>", plan =
// the account's plan), windows 5h, 7d and 30d.
//
// Polled: OpenCode's usage API (undocumented; OpenCode's own client reads it), with the account's
// key, every TMX_QUOTA_POLL_MINUTES:
//
//	GET https://opencode.ai/zen/go/v1/usage   (also /inference/go/v1/usage)
//	{"usage":{"rolling":{"status":"ok","percent":2,"resetsAt":"2026-11-04T08:36:27.000Z"},
//	          "weekly":{...}, "monthly":{...}}}
//
// rolling → 5h, weekly → 7d, monthly → 30d; status "ok" or "rate-limited" (OpenCode's own client
// schema). It counts every client of the account (another proxy, OpenCode's app), so it wins.
// Between polls the reading is the last poll plus the allowance this proxy logged since
// (internal/tmx/opencode), source passive, so a burst shows before the next poll. When the API is
// gone (404, 403) or answers another shape, poll_error says so and every window reads the allowance
// estimate at once; a transient error (network, 5xx, 429) keeps the last good poll until each
// window's reset.
//
// Polled readings reach routing on the account's credential (reset-soonest ranks by the weekly
// reset; a rate-limited window makes the credential a last resort). They never create a cooldown:
// that stays OpenCode's own 429 GoUsageLimitError, whose Retry-After cools the credential.

var openCodeUsagePaths = []string{"/zen/go/v1/usage", "/inference/go/v1/usage"}

// openCodeGonePause backs off an account whose usage API is missing or refuses the key.
const openCodeGonePause = time.Hour

// openCodeMeters maps the usage API's meters to window names.
var openCodeMeters = []struct{ key, window string }{{"rolling", "5h"}, {"weekly", "7d"}, {"monthly", "30d"}}

// openCodePoll is one OpenCode account's last good usage read.
type openCodePoll struct {
	at      time.Time
	windows []Window
}

// OpenCode returns the OpenCode quota settings store, or nil.
func (s *Service) OpenCode() *opencode.Store {
	if s == nil {
		return nil
	}
	return s.oc
}

// ParseOpenCodeUsage maps OpenCode's usage reply to windows. percent is a whole number (0 means
// under 1%, still a reading). A missing meter, percent or reset, or a status other than ok and
// rate-limited, is an error (the API changed).
func ParseOpenCodeUsage(body []byte) ([]Window, error) {
	var p struct {
		Usage map[string]*struct {
			Status   string   `json:"status"`
			Percent  *float64 `json:"percent"`
			ResetsAt string   `json:"resetsAt"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(body, &p); err != nil {
		return nil, err
	}
	out := make([]Window, 0, len(openCodeMeters))
	for _, m := range openCodeMeters {
		v := p.Usage[m.key]
		if v == nil || v.Percent == nil || math.IsNaN(*v.Percent) || math.IsInf(*v.Percent, 0) || *v.Percent < 0 {
			return nil, fmt.Errorf("usage.%s missing or without a percent", m.key)
		}
		reset, err := time.Parse(time.RFC3339, v.ResetsAt)
		if err != nil {
			return nil, fmt.Errorf("usage.%s.resetsAt: %w", m.key, err)
		}
		status := strings.TrimSpace(v.Status)
		if status != "ok" && status != "rate-limited" {
			return nil, fmt.Errorf("usage.%s.status %q is neither ok nor rate-limited", m.key, v.Status)
		}
		pct := *v.Percent
		reset = reset.UTC()
		out = append(out, Window{Window: m.window, UsedPct: &pct, ResetsAt: &reset, Status: openCodeStatus(v.Status, pct)})
	}
	return out, nil
}

// openCodeStatus: "rate-limited" is OpenCode refusing the account; "ok" (or an estimate) is graded
// by percent, never as rejected: only OpenCode says a window is spent.
func openCodeStatus(status string, pct float64) string {
	switch {
	case strings.EqualFold(strings.TrimSpace(status), "rate-limited"):
		return opencode.StatusAtLimit
	case pct >= 80:
		return opencode.StatusWarning
	default:
		return opencode.StatusAllowed
	}
}

// mergeOpenCode is an account's reading at now from its last poll and the allowance reading
// (passive, with each window's cap). A polled window still open reads the poll plus sinceUSD (the
// allowance logged after the poll) over its cap; it is returned only when that adds something, as
// the poll itself is already recorded. A window the poll lacks or whose polled reset has passed
// reads the allowance estimate.
func mergeOpenCode(poll *openCodePoll, passive []opencode.WindowReading, sinceUSD float64, now time.Time) []Window {
	caps := make(map[string]*float64, len(passive))
	for _, w := range passive {
		caps[w.Window] = w.CapUSD
	}
	polled := map[string]bool{}
	var out []Window
	if poll != nil {
		for _, w := range poll.windows {
			if w.ResetsAt == nil || !w.ResetsAt.After(now) {
				continue
			}
			polled[w.Window] = true
			capUSD := caps[w.Window]
			if sinceUSD <= 0 || capUSD == nil || *capUSD <= 0 || w.UsedPct == nil {
				continue
			}
			pct := *w.UsedPct + 100*sinceUSD/(*capUSD)
			status := w.Status
			if status != opencode.StatusAtLimit {
				status = openCodeStatus("", pct)
			}
			out = append(out, Window{Window: w.Window, UsedPct: &pct, ResetsAt: w.ResetsAt, Status: status})
		}
	}
	for _, w := range passive {
		if !polled[w.Window] {
			out = append(out, Window{Window: w.Window, UsedPct: w.UsedPct, ResetsAt: w.ResetsAt, Status: w.Status})
		}
	}
	return out
}

// openCodeCredentials maps each account of st to one credential carrying its key: the entry's
// enabled API-key credentials, matched by key fingerprint (any one when st has no accounts).
func openCodeCredentials(auths []*coreauth.Auth, st opencode.Settings) map[string]*coreauth.Auth {
	byKey := map[string]string{}
	for _, a := range st.Accounts {
		for _, k := range a.Keys {
			byKey[k] = a.Name
		}
	}
	sorted := append([]*coreauth.Auth(nil), auths...)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i] != nil && sorted[j] != nil && sorted[i].ID < sorted[j].ID })
	out := map[string]*coreauth.Auth{}
	for _, auth := range sorted {
		if auth == nil || auth.Disabled || auth.Attributes == nil ||
			!strings.EqualFold(strings.TrimSpace(auth.Attributes["compat_name"]), st.Entry) {
			continue
		}
		key := strings.TrimSpace(auth.Attributes["api_key"])
		if key == "" {
			continue
		}
		name := ""
		if len(st.Accounts) > 0 {
			var ok bool
			if name, ok = byKey[requestlog.KeyFingerprint(key)]; !ok {
				continue
			}
		}
		if _, taken := out[name]; !taken {
			out[name] = auth
		}
	}
	return out
}

func (s *Service) tickOpenCode(ctx context.Context, now time.Time, seen map[string]bool, auths []*coreauth.Auth) {
	if s.oc == nil {
		return
	}
	subs, err := s.oc.List(ctx)
	if err != nil {
		log.WithError(err).Warn("tmx quota: reading OpenCode settings failed")
		return
	}
	for _, st := range subs {
		readings, errR := s.oc.ReadAll(ctx, st, now)
		if errR != nil {
			log.WithError(errR).Warnf("tmx quota: OpenCode readings for %s failed", st.Entry)
			continue
		}
		creds := openCodeCredentials(auths, st)
		for _, r := range readings {
			id := opencode.AccountID(st.Entry, r.Account)
			label := "OpenCode Go"
			if r.Account != "" {
				label += " · " + r.Account
			}
			seen[id] = true
			cred := creds[r.Account]
			s.mu.Lock()
			a := s.accountLocked(id)
			a.present, a.Provider, a.Label, a.Plan = true, st.Entry, label, r.Plan
			due := cred != nil && s.openCodePollDueLocked(a, now) && s.reservePollLocked(a)
			s.mu.Unlock()
			if due {
				s.pollOpenCode(ctx, id, cred, a)
			}
			s.mu.Lock()
			poll := a.ocPoll
			s.mu.Unlock()
			since := 0.0
			if poll != nil {
				if since, err = s.oc.AllowanceSince(ctx, st, r.Account, poll.at, now); err != nil {
					log.WithError(err).Warnf("tmx quota: OpenCode allowance since the last poll for %s failed", id)
					since = 0
				}
			}
			for _, w := range mergeOpenCode(poll, r.Windows, since, now) {
				s.record(ctx, id, []Window{w}, "passive", now.UTC())
			}
		}
	}
}

// openCodePollDueLocked: never polled, an interval (plus jitter) since the last poll, or a polled
// window has reset since the last poll (once: a failed re-read waits for the interval); never while
// paused or polling.
func (s *Service) openCodePollDueLocked(a *account, now time.Time) bool {
	if a.polling != nil || now.Before(a.pauseUntil) {
		return false
	}
	if a.LastPollAt == nil {
		return true
	}
	since := now.Sub(*a.LastPollAt)
	if since >= s.pollEvery+pollJitter(a.AuthID, s.pollEvery) {
		return true
	}
	if a.ocPoll != nil && since >= pollNowMinGap {
		for _, w := range a.ocPoll.windows {
			if w.ResetsAt != nil && !w.ResetsAt.After(now) && a.LastPollAt.Before(*w.ResetsAt) {
				return true
			}
		}
	}
	return false
}

// pollOpenCode reads one account's usage with its credential (the caller reserved the poll).
func (s *Service) pollOpenCode(ctx context.Context, id string, cred *coreauth.Auth, a *account) {
	defer s.finishPoll(a)
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	now := s.now().UTC()
	status, body, err := ReadUsage(ctx, cred, func(readCtx context.Context) (int, []byte, error) {
		return s.fetchOpenCodeUsage(readCtx, cred)
	})
	var windows []Window
	// gone: the API is missing or answers another shape, so the last good poll is dropped and every
	// window reads the allowance estimate (a transient error keeps it until each window's reset).
	pollErr, pause, gone := "", time.Duration(0), false
	const fallback = "; showing the allowance estimate"
	switch {
	case err != nil:
		pollErr = err.Error()
	case status == 429:
		pollErr, pause = "rate limited (429)", rateLimitPause
	case status == 401:
		pollErr, pause = "unauthorized (401)", openCodeGonePause
	case status == 403:
		pollErr, pause, gone = "no Go usage for this key (403)"+fallback, openCodeGonePause, true
	case status == 404:
		pollErr, pause, gone = "usage API not found (404)"+fallback, openCodeGonePause, true
	case status < 200 || status >= 300:
		pollErr = fmt.Sprintf("status %d", status)
	default:
		if windows, err = ParseOpenCodeUsage(body); err != nil {
			pollErr, gone = "unexpected usage reply: "+err.Error()+fallback, true
		}
	}
	s.mu.Lock()
	a.LastPollAt, a.PollError = &now, pollErr
	if pause > 0 {
		a.pauseUntil = now.Add(pause)
	}
	if pollErr == "" {
		a.ocPoll = &openCodePoll{at: now, windows: windows}
	} else if gone {
		a.ocPoll = nil
	}
	s.mu.Unlock()
	if pollErr != "" {
		log.WithField("auth_id", id).Debugf("tmx quota: OpenCode usage poll failed: %s", pollErr)
		return
	}
	s.applyRouting(cred.ID, cred.Provider, windows, now)
	s.record(ctx, id, windows, "polled", now)
}

// fetchOpenCodeUsage GETs the usage API at the entry's origin, trying the next path on a 404.
func (s *Service) fetchOpenCodeUsage(ctx context.Context, cred *coreauth.Auth) (int, []byte, error) {
	base, err := url.Parse(strings.TrimSpace(cred.Attributes["base_url"]))
	if err != nil || base.Scheme == "" || base.Host == "" {
		return 0, nil, fmt.Errorf("entry has no usable base URL")
	}
	headers := map[string]string{
		"Authorization": "Bearer " + strings.TrimSpace(cred.Attributes["api_key"]),
		"Accept":        "application/json",
		"User-Agent":    "tokenmaxx-proxy/1.0",
	}
	var status int
	var body []byte
	for _, path := range openCodeUsagePaths {
		status, body, err = s.fetch.TmxFetch(ctx, cred, base.Scheme+"://"+base.Host+path, headers)
		if err != nil || status != 404 {
			break
		}
	}
	return status, body, err
}
