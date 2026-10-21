// Package resets lists and redeems banked rate-limit reset credits (tokenmaxx fork).
//
// Codex: chatgpt.com/backend-api/wham/usage + /wham/rate-limit-reset-credits, redeemed with
// POST /wham/rate-limit-reset-credits/consume. Claude: the cedar_ember block of
// api.anthropic.com/api/oauth/usage, redeemed with POST /api/organizations/<org>/reset_rate_limits.
// These endpoints are undocumented (they are what the official CLIs call) and may change; parsers
// fail closed.
//
// Spending is safe to retry: every claim is written to <schema>.reset_redemptions with its request
// id before it is sent. Codex ids are a UUIDv5 of account:credit, so any retry from anywhere is the
// same claim; a Claude id is reused while the claim has no definite answer (24 h). One redeem per
// account at a time. After a reset the proxy's cooldown is cleared and its passive quota signals
// dropped so reset-soonest re-ranks the account, then the account is re-read to confirm.
package resets

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/quota"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	log "github.com/sirupsen/logrus"
)

const (
	cacheTTL       = 5 * time.Minute
	refreshMinGap  = 30 * time.Second
	readTimeout    = 15 * time.Second
	codexTimeout   = 20 * time.Second
	claudeTimeout  = 25 * time.Second
	pendingReuse   = 24 * time.Hour
	readParallel   = 4
	historyDefault = 20

	claudeStatusURL  = "https://api.anthropic.com/api/oauth/usage?cedar_ember=1&skip_spend=1"
	claudeProfileURL = "https://api.anthropic.com/api/oauth/profile"
	claudeRedeemURL  = "https://api.anthropic.com/api/organizations/%s/reset_rate_limits"
	codexUsageURL    = "https://chatgpt.com/backend-api/wham/usage"
	codexCreditsURL  = "https://chatgpt.com/backend-api/wham/rate-limit-reset-credits"
	codexRedeemURL   = "https://chatgpt.com/backend-api/wham/rate-limit-reset-credits/consume"
)

// codexRedeemNamespace makes Codex redeem ids deterministic per account:credit. Same namespace as
// t3code, so a t3code hub redeem and ours for the same credit are one claim upstream.
var codexRedeemNamespace = uuid.MustParse("6f1c2a9e-2d4b-4c1e-9a7f-3b8d5e0c1a42")

var orgUUIDRe = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// Outcomes of a redeem.
const (
	OutcomeReset           = "reset"
	OutcomeAlreadyRedeemed = "already_redeemed"
	OutcomeNothingToReset  = "nothing_to_reset"
	OutcomeNoCredit        = "no_credit"
	OutcomeIneligible      = "ineligible"
	OutcomeCooldown        = "cooldown"
	OutcomeRateLimited     = "rate_limited"
	OutcomeAuthError       = "auth_error"
	OutcomeRejected        = "rejected"
	OutcomeUnknown         = "unknown"
)

// Errors returned by Redeem.
var (
	ErrUnknownAccount = errors.New("unknown account")
	ErrUnsupported    = errors.New("account does not bank reset credits")
	ErrBusy           = errors.New("a redeem for this account is already running")
	ErrReadFailed     = errors.New("could not read the account's reset credits")
	ErrNoOrganization = errors.New("claude organization unknown; sign in again")
)

// BlockedError: the provider would refuse this credit now (Code is a Credit.Blocker value, or
// "unknown_credit").
type BlockedError struct{ Code string }

func (e *BlockedError) Error() string { return "credit cannot be used now: " + e.Code }

// Backend is what the management handler provides.
type Backend interface {
	TmxAuths() []*coreauth.Auth
	TmxSend(ctx context.Context, auth *coreauth.Auth, method, url string, headers map[string]string, body []byte) (int, []byte, error)
	TmxResetCooldown(ctx context.Context, authID string) error
	TmxClearQuotaSignals(authID string) bool
}

// Account is one account's reset-credit state.
type Account struct {
	AuthID    string     `json:"auth_id"`
	AuthIndex string     `json:"auth_index"`
	Provider  string     `json:"provider"`
	Label     string     `json:"label"`
	Plan      string     `json:"plan"` // from quota tracking, filled by the API layer
	CheckedAt *time.Time `json:"checked_at"`
	Error     string     `json:"error,omitempty"`
	Status
	Pending *Redemption `json:"pending,omitempty"`
}

// Redemption is one claim, as journaled.
type Redemption struct {
	ID          int64      `json:"id"`
	CreatedAt   time.Time  `json:"created_at"`
	SettledAt   *time.Time `json:"settled_at,omitempty"`
	AuthID      string     `json:"auth_id"`
	Provider    string     `json:"provider"`
	Label       string     `json:"label"`
	CreditID    string     `json:"credit_id"`
	CreditTitle string     `json:"credit_title"`
	RequestID   string     `json:"-"`
	State       string     `json:"state"` // pending (no definite answer; retry reuses the id) | settled
	Outcome     string     `json:"outcome"`
	Confirmed   bool       `json:"confirmed"`
	Warning     string     `json:"warning,omitempty"`
	Before      []Window   `json:"before"`
	After       []Window   `json:"after"`
	Actor       string     `json:"actor,omitempty"`
}

type cached struct {
	acct   Account
	readAt time.Time
}

// Service holds the credit inventory cache and the claim journal.
type Service struct {
	backend Backend
	journal journal
	now     func() time.Time

	mu    sync.Mutex
	cache map[string]*cached
	locks map[string]*sync.Mutex
}

var (
	defaultMu sync.Mutex
	current   *Service
)

// Default returns the running service, or nil.
func Default() *Service {
	defaultMu.Lock()
	defer defaultMu.Unlock()
	return current
}

// SetDefaultForTest replaces the running service (nil clears it) so route tests in one process
// don't inherit a service started by an earlier test with a different backend.
func SetDefaultForTest(s *Service) {
	defaultMu.Lock()
	defer defaultMu.Unlock()
	current = s
}

// Start creates the journal table and the service once. db/schema are the request-log database
// ("tmx"); without a db the journal is in memory. TMX_RESETS=off disables it.
func Start(db *sql.DB, schema string, backend Backend) *Service {
	defaultMu.Lock()
	defer defaultMu.Unlock()
	if current != nil || backend == nil {
		return current
	}
	if strings.EqualFold(strings.TrimSpace(os.Getenv("TMX_RESETS")), "off") {
		log.Info("tmx resets: disabled (TMX_RESETS=off)")
		return nil
	}
	var j journal = newMemJournal()
	if db != nil {
		pj, err := newPGJournal(context.Background(), db, schema)
		if err != nil {
			log.WithError(err).Warn("tmx resets: not started")
			return nil
		}
		j = pj
	}
	current = New(backend, j)
	log.Info("tmx resets: ready")
	return current
}

// New builds a service (tests use it with a memory journal).
func New(backend Backend, j journal) *Service {
	if j == nil {
		j = newMemJournal()
	}
	return &Service{backend: backend, journal: j, now: time.Now,
		cache: map[string]*cached{}, locks: map[string]*sync.Mutex{}}
}

func providerOf(auth *coreauth.Auth) (string, bool) {
	if auth == nil || auth.Disabled {
		return "", false
	}
	p := strings.ToLower(strings.TrimSpace(auth.Provider))
	switch p {
	case "claude":
		return p, true
	case "codex":
		if auth.Attributes != nil && strings.TrimSpace(auth.Attributes["api_key"]) != "" {
			return "", false
		}
		return p, true
	}
	return "", false
}

func labelOf(auth *coreauth.Auth) string {
	if l := strings.TrimSpace(auth.Label); l != "" {
		return l
	}
	return auth.ID
}

func metaString(auth *coreauth.Auth, key string) string {
	if auth == nil || auth.Metadata == nil {
		return ""
	}
	s, _ := auth.Metadata[key].(string)
	return strings.TrimSpace(s)
}

func claudeHeaders() map[string]string {
	return map[string]string{
		"User-Agent":     "claude-cli/2.1.280 (external, cli)",
		"Authorization":  "Bearer $TOKEN$",
		"Content-Type":   "application/json",
		"Accept":         "application/json",
		"anthropic-beta": "oauth-2025-04-20",
	}
}

func codexHeaders(auth *coreauth.Auth) map[string]string {
	h := map[string]string{
		"Authorization": "Bearer $TOKEN$",
		"Content-Type":  "application/json",
		"Accept":        "application/json",
		"OpenAI-Beta":   "codex-1",
		"Originator":    "Codex Desktop",
		"User-Agent":    "codex-tui/0.149.1 (Mac OS 26.5.2; arm64) iTerm.app/3.6.11 (codex-tui; 0.149.1)",
	}
	if id := metaString(auth, "account_id"); id != "" {
		h["Chatgpt-Account-Id"] = id
	}
	return h
}

// httpError maps a failed read to fixed text; upstream bodies are never echoed (they can carry
// tokens).
func httpError(code int, err error) string {
	switch {
	case err != nil:
		return "request_failed"
	case code == 429:
		return "rate_limited"
	case code == 401 || code == 403:
		return "auth_error"
	}
	return fmt.Sprintf("upstream_%d", code)
}

// read fetches one account's credits from its provider.
func (s *Service) read(ctx context.Context, auth *coreauth.Auth, provider string) Account {
	now := s.now().UTC()
	a := Account{AuthID: auth.ID, AuthIndex: auth.EnsureIndex(), Provider: provider, Label: labelOf(auth),
		CheckedAt: &now, Status: Status{Windows: []Window{}, Credits: []Credit{}}}
	ctx, cancel := context.WithTimeout(ctx, readTimeout)
	defer cancel()
	switch provider {
	case "claude":
		code, body, err := quota.ReadUsage(ctx, auth, func(readCtx context.Context) (int, []byte, error) {
			return s.backend.TmxSend(readCtx, auth, "GET", claudeStatusURL, claudeHeaders(), nil)
		})
		if err != nil || code < 200 || code >= 300 {
			a.Error = httpError(code, err)
			return a
		}
		st, errParse := ParseClaude(body, now)
		if errParse != nil {
			a.Error = "malformed"
			return a
		}
		a.Status = st
	case "codex":
		h := codexHeaders(auth)
		code, body, err := quota.ReadUsage(ctx, auth, func(readCtx context.Context) (int, []byte, error) {
			return s.backend.TmxSend(readCtx, auth, "GET", codexUsageURL, h, nil)
		})
		if err != nil || code < 200 || code >= 300 {
			a.Error = httpError(code, err)
			return a
		}
		st, errParse := ParseCodexUsage(body, now)
		if errParse != nil {
			a.Error = "malformed"
			return a
		}
		code, body, err = s.backend.TmxSend(ctx, auth, "GET", codexCreditsURL, h, nil)
		if err != nil || code < 200 || code >= 300 {
			a.Status = st
			a.Error = httpError(code, err)
			return a
		}
		if errParse = ParseCodexCredits(body, &st, now); errParse != nil {
			a.Status = st
			a.Error = "malformed"
			return a
		}
		a.Status = st
	}
	return a
}

// Accounts returns every Claude/Codex account's credits. Readings are cached for 5 minutes;
// refresh re-reads accounts not read in the last 30 s.
func (s *Service) Accounts(ctx context.Context, refresh bool) []Account {
	type job struct {
		auth     *coreauth.Auth
		provider string
	}
	now := s.now()
	var jobs []job
	keep := map[string]bool{}
	var order []string
	s.mu.Lock()
	for _, auth := range s.backend.TmxAuths() {
		provider, ok := providerOf(auth)
		if !ok {
			continue
		}
		keep[auth.ID] = true
		order = append(order, auth.ID)
		c := s.cache[auth.ID]
		if c == nil || now.Sub(c.readAt) >= cacheTTL || (refresh && now.Sub(c.readAt) >= refreshMinGap) {
			jobs = append(jobs, job{auth, provider})
		}
	}
	for id := range s.cache {
		if !keep[id] {
			delete(s.cache, id)
		}
	}
	s.mu.Unlock()

	sem := make(chan struct{}, readParallel)
	var wg sync.WaitGroup
	for _, j := range jobs {
		wg.Add(1)
		sem <- struct{}{}
		go func(j job) {
			defer func() { <-sem; wg.Done() }()
			acct := s.read(ctx, j.auth, j.provider)
			s.store(acct)
		}(j)
	}
	wg.Wait()

	pending, err := s.journal.unsettled(ctx, s.now().Add(-pendingReuse))
	if err != nil {
		log.WithError(err).Warn("tmx resets: reading pending claims")
	}
	out := make([]Account, 0, len(order))
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range order {
		c := s.cache[id]
		if c == nil {
			continue
		}
		out = append(out, s.view(c.acct, pending))
	}
	return out
}

// view is a response copy of a cached account: pending claim attached, verdicts filled.
func (s *Service) view(cachedAcct Account, pending []Redemption) Account {
	acct := own(cachedAcct)
	acct.Pending = nil
	for i := range pending {
		if pending[i].AuthID == acct.AuthID {
			p := pending[i]
			acct.Pending = &p
			break
		}
	}
	decorate(&acct, s.now())
	return acct
}

// PollNow re-reads one account and returns it. Throttled (read within the last 30 s) returns the
// cached reading without a provider call.
func (s *Service) PollNow(ctx context.Context, authID string) (*Account, bool, error) {
	auth, provider, err := s.findAuth(strings.TrimSpace(authID))
	if err != nil {
		return nil, false, err
	}
	s.mu.Lock()
	c := s.cache[auth.ID]
	throttled := c != nil && s.now().Sub(c.readAt) < refreshMinGap
	s.mu.Unlock()
	if !throttled {
		s.store(s.read(ctx, auth, provider))
	}
	pending, errPending := s.journal.unsettled(ctx, s.now().Add(-pendingReuse))
	if errPending != nil {
		log.WithError(errPending).Warn("tmx resets: reading pending claims")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	c = s.cache[auth.ID]
	if c == nil {
		return nil, throttled, ErrReadFailed
	}
	acct := s.view(c.acct, pending)
	return &acct, throttled, nil
}

// store caches a reading, keeping the previous credits when the new read failed outright.
func (s *Service) store(acct Account) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if prev := s.cache[acct.AuthID]; prev != nil && acct.Error != "" && len(acct.Windows) == 0 && len(acct.Credits) == 0 {
		kept := prev.acct
		kept.Error = acct.Error
		s.cache[acct.AuthID] = &cached{acct: kept, readAt: s.now()}
		return
	}
	s.cache[acct.AuthID] = &cached{acct: acct, readAt: s.now()}
}

// History returns claims matching q, newest first.
func (s *Service) History(ctx context.Context, q HistoryQuery) ([]Redemption, error) {
	if q.Limit <= 0 || q.Limit > 500 {
		q.Limit = historyDefault
	}
	return s.journal.list(ctx, q)
}

func (s *Service) lockFor(authID string) *sync.Mutex {
	s.mu.Lock()
	defer s.mu.Unlock()
	l := s.locks[authID]
	if l == nil {
		l = &sync.Mutex{}
		s.locks[authID] = l
	}
	return l
}

func (s *Service) findAuth(authID string) (*coreauth.Auth, string, error) {
	for _, auth := range s.backend.TmxAuths() {
		if auth == nil || auth.ID != authID {
			continue
		}
		provider, ok := providerOf(auth)
		if !ok {
			return nil, "", ErrUnsupported
		}
		return auth, provider, nil
	}
	return nil, "", ErrUnknownAccount
}

// Result of a redeem: the journaled claim and the account as re-read afterwards.
type Result struct {
	Redemption Redemption `json:"redemption"`
	Account    *Account   `json:"account,omitempty"`
}

func settles(outcome string) bool {
	return outcome == OutcomeReset || outcome == OutcomeAlreadyRedeemed
}

// Redeem spends one credit. A claim that got no definite answer stays pending and Redeem with the
// same credit retries it under the same request id.
func (s *Service) Redeem(ctx context.Context, authID, creditID, actor string) (*Result, error) {
	authID, creditID = strings.TrimSpace(authID), strings.TrimSpace(creditID)
	auth, provider, err := s.findAuth(authID)
	if err != nil {
		return nil, err
	}
	if !creditIDRe.MatchString(creditID) || (provider == "claude" && !claudeGrantIDRe.MatchString(creditID)) {
		return nil, &BlockedError{Code: "unknown_credit"}
	}
	lock := s.lockFor(authID)
	if !lock.TryLock() {
		return nil, ErrBusy
	}
	defer lock.Unlock()

	prior, err := s.journal.pending(ctx, authID, creditID, s.now().Add(-pendingReuse))
	if err != nil {
		return nil, fmt.Errorf("reading claim journal: %w", err)
	}
	before := s.read(ctx, auth, provider)
	s.store(before)
	clears := []string(nil)
	title := ""
	if prior == nil {
		if before.Error != "" {
			return nil, ErrReadFailed
		}
		var credit *Credit
		for i := range before.Credits {
			if before.Credits[i].ID == creditID {
				credit = &before.Credits[i]
			}
		}
		if credit == nil {
			return nil, &BlockedError{Code: "unknown_credit"}
		}
		if credit.Blocker != "" {
			return nil, &BlockedError{Code: credit.Blocker}
		}
		clears, title = credit.Clears, credit.Title
	} else {
		title = prior.CreditTitle
	}

	org := ""
	if provider == "claude" {
		if org = s.claudeOrganization(ctx, auth); org == "" {
			return nil, ErrNoOrganization
		}
	}

	r := prior
	if r == nil {
		requestID := uuid.NewString()
		if provider == "codex" {
			account := metaString(auth, "account_id")
			if account == "" {
				account = auth.ID
			}
			requestID = uuid.NewSHA1(codexRedeemNamespace, []byte(account+":"+creditID)).String()
		}
		r = &Redemption{CreatedAt: s.now().UTC(), AuthID: authID, Provider: provider, Label: labelOf(auth),
			CreditID: creditID, CreditTitle: title, RequestID: requestID, State: "pending",
			Before: before.Windows, After: []Window{}, Actor: actor}
		if err = s.journal.insert(ctx, r); err != nil {
			return nil, fmt.Errorf("journaling claim: %w", err)
		}
	}

	// Detached from the HTTP request: the answer must be journaled even if the caller goes away.
	sendCtx, cancel := context.WithTimeout(context.Background(), map[string]time.Duration{
		"codex": codexTimeout, "claude": claudeTimeout}[provider])
	outcome := OutcomeUnknown
	if provider == "codex" {
		outcome = s.sendCodex(sendCtx, auth, creditID, r.RequestID)
	} else {
		outcome = s.sendClaude(sendCtx, auth, org, creditID, r.RequestID)
	}
	cancel()

	r.Outcome = outcome
	// A refusal on a retry can't prove the earlier ambiguous claim didn't spend: stay pending.
	if outcome != OutcomeUnknown && (prior == nil || settles(outcome)) {
		at := s.now().UTC()
		r.State, r.SettledAt = "settled", &at
	}
	res := &Result{}
	if settles(outcome) {
		if errReset := s.backend.TmxResetCooldown(ctx, authID); errReset != nil {
			log.WithError(errReset).Warn("tmx resets: cooldown reset after redeem")
			r.Warning = "cooldown_not_cleared"
		}
		s.backend.TmxClearQuotaSignals(authID)
		after := s.read(context.Background(), auth, provider)
		s.store(after)
		if after.Error == "" {
			r.After = after.Windows
			r.Confirmed = confirmed(r.Before, after.Windows, clears)
		}
		if !r.Confirmed && r.Warning == "" {
			r.Warning = "unconfirmed"
		}
	}
	if err = s.journal.settle(context.Background(), r); err != nil {
		log.WithError(err).Warn("tmx resets: journaling claim answer")
	}
	res.Redemption = *r
	var pending []Redemption
	if r.State == "pending" {
		pending = []Redemption{*r}
	}
	s.mu.Lock()
	if c := s.cache[authID]; c != nil {
		acct := s.view(c.acct, pending)
		res.Account = &acct
	}
	s.mu.Unlock()
	return res, nil
}

// confirmed: every cleared window that was read before and after now reads (nearly) empty or
// lower than before.
func confirmed(before, after []Window, clears []string) bool {
	if len(after) == 0 {
		return false
	}
	want := map[string]bool{}
	for _, c := range clears {
		want[c] = true
	}
	prev := map[string]*float64{}
	for _, w := range before {
		prev[w.Window] = w.UsedPct
	}
	checked := 0
	for _, w := range after {
		if (len(want) > 0 && !want[w.Window]) || w.UsedPct == nil {
			continue
		}
		checked++
		if *w.UsedPct <= 1 {
			continue
		}
		if p := prev[w.Window]; p != nil && *w.UsedPct < *p-0.5 {
			continue
		}
		return false
	}
	return checked > 0
}

func (s *Service) claudeOrganization(ctx context.Context, auth *coreauth.Auth) string {
	if org := metaString(auth, "organization_uuid"); orgUUIDRe.MatchString(org) {
		return strings.ToLower(org)
	}
	ctx, cancel := context.WithTimeout(ctx, readTimeout)
	defer cancel()
	code, body, err := s.backend.TmxSend(ctx, auth, "GET", claudeProfileURL, claudeHeaders(), nil)
	if err != nil || code < 200 || code >= 300 {
		return ""
	}
	var profile struct {
		Organization struct {
			UUID string `json:"uuid"`
		} `json:"organization"`
	}
	if json.Unmarshal(body, &profile) != nil || !orgUUIDRe.MatchString(profile.Organization.UUID) {
		return ""
	}
	return strings.ToLower(profile.Organization.UUID)
}

func refusal(code int) string {
	switch {
	case code == 429:
		return OutcomeRateLimited
	case code == 401 || code == 403:
		return OutcomeAuthError
	case code >= 400 && code < 500:
		return OutcomeRejected
	}
	return OutcomeUnknown
}

func (s *Service) sendCodex(ctx context.Context, auth *coreauth.Auth, creditID, requestID string) string {
	body, _ := json.Marshal(map[string]string{"redeem_request_id": requestID, "credit_id": creditID})
	code, resp, err := s.backend.TmxSend(ctx, auth, "POST", codexRedeemURL, codexHeaders(auth), body)
	if err != nil {
		return OutcomeUnknown
	}
	if code < 200 || code >= 300 {
		return refusal(code)
	}
	var out struct {
		Code string `json:"code"`
	}
	if json.Unmarshal(resp, &out) != nil {
		return OutcomeUnknown
	}
	switch out.Code {
	case "reset":
		return OutcomeReset
	case "already_redeemed":
		return OutcomeAlreadyRedeemed
	case "nothing_to_reset":
		return OutcomeNothingToReset
	case "no_credit":
		return OutcomeNoCredit
	}
	return OutcomeUnknown
}

func (s *Service) sendClaude(ctx context.Context, auth *coreauth.Auth, org, grantID, requestID string) string {
	body, _ := json.Marshal(map[string]string{"program": "cedar_ember", "grant_id": grantID, "request_id": requestID})
	code, resp, err := s.backend.TmxSend(ctx, auth, "POST", fmt.Sprintf(claudeRedeemURL, org), claudeHeaders(), body)
	if err != nil {
		return OutcomeUnknown
	}
	if code < 200 || code >= 300 {
		return refusal(code)
	}
	var out struct {
		Result string `json:"result"`
	}
	if json.Unmarshal(resp, &out) != nil {
		return OutcomeUnknown
	}
	switch out.Result {
	case "reset":
		return OutcomeReset
	case "already_used":
		return OutcomeAlreadyRedeemed
	case "not_limited":
		return OutcomeNothingToReset
	case "ineligible":
		return OutcomeIneligible
	case "cooldown":
		return OutcomeCooldown
	}
	return OutcomeUnknown // includes "unavailable": no definite answer, retry with the same id
}
