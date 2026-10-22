package api

// tokenmaxx fork: management routes for banked reset credits (internal/tmx/resets).
// Contract for fleet and the UI: wiki/reset-credits.md.

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/quota"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/requestlog"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/resets"
)

func registerTMXResetRoutes(v8 *gin.RouterGroup, backend resets.Backend) {
	if store := requestlog.Default(); store != nil {
		resets.Start(store.DB(), store.Schema(), backend)
	} else {
		resets.Start(nil, "", backend)
	}
	v8.GET("/tmx/resets", tmxResetsList)
	v8.POST("/tmx/resets/poll", tmxResetsPoll)
	v8.GET("/tmx/resets/history", tmxResetsHistory)
	v8.GET("/tmx/resets/balance-history", tmxResetsBalanceHistory)
	v8.POST("/tmx/resets/redeem", tmxResetsRedeem)
}

// withPlans fills Account.Plan from quota tracking (same labels as /tmx/quota/current).
func withPlans(accounts ...*resets.Account) {
	q := quota.Default()
	if q == nil {
		return
	}
	plans := map[string]string{}
	for _, a := range q.Accounts() {
		plans[a.AuthID] = a.Plan
	}
	for _, a := range accounts {
		if a != nil {
			a.Plan = plans[a.AuthID]
		}
	}
}

// tmxResetsList: GET /tmx/resets[?refresh=1&auth_id&provider&history=N] — every Claude/Codex
// account's banked credits (soonest expiry first) with a verdict per credit, a summary, and the N
// most recent claims (default 20, 0 = none).
func tmxResetsList(c *gin.Context) {
	svc := resets.Default()
	if svc == nil {
		c.JSON(http.StatusOK, gin.H{"enabled": false, "accounts": []any{}, "history": []any{}})
		return
	}
	refresh := c.Query("refresh") == "1" || c.Query("refresh") == "true"
	authID, provider := strings.TrimSpace(c.Query("auth_id")), strings.ToLower(strings.TrimSpace(c.Query("provider")))
	all := svc.Accounts(c.Request.Context(), refresh)
	accounts := make([]resets.Account, 0, len(all))
	for _, a := range all {
		if (authID == "" || a.AuthID == authID) && (provider == "" || a.Provider == provider) {
			accounts = append(accounts, a)
		}
	}
	for i := range accounts {
		withPlans(&accounts[i])
	}
	history := []resets.Redemption{}
	if n, err := strconv.Atoi(c.DefaultQuery("history", "20")); err == nil && n > 0 {
		if rows, errHistory := svc.History(c.Request.Context(), resets.HistoryQuery{AuthID: authID, Provider: provider, Limit: n}); errHistory == nil && rows != nil {
			history = rows
		}
	}
	now := time.Now().UTC()
	c.JSON(http.StatusOK, gin.H{"enabled": true, "generated_at": now, "summary": resets.Summarize(accounts, now),
		"accounts": accounts, "history": history})
}

// tmxResetsPoll: POST /tmx/resets/poll?auth_id=… — re-read one account now; {account, throttled}.
// Throttled (read within 30 s) returns the cached reading without a provider call.
func tmxResetsPoll(c *gin.Context) {
	svc := resets.Default()
	if svc == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "reset credits are off (TMX_RESETS)"})
		return
	}
	acct, throttled, err := svc.PollNow(c.Request.Context(), c.Query("auth_id"))
	if tmxResetsError(c, err) {
		return
	}
	withPlans(acct)
	c.JSON(http.StatusOK, gin.H{"account": acct, "throttled": throttled})
}

// tmxResetsHistory: GET /tmx/resets/history?auth_id&provider&outcome&state&from&to&before_id&limit
// — journaled claims, newest first (limit ≤ 500, default 20); next_before_id is 0 on the last page.
func tmxResetsHistory(c *gin.Context) {
	svc := resets.Default()
	if svc == nil {
		c.JSON(http.StatusOK, gin.H{"enabled": false, "items": []any{}, "next_before_id": 0})
		return
	}
	q := resets.HistoryQuery{
		AuthID:   strings.TrimSpace(c.Query("auth_id")),
		Provider: strings.ToLower(strings.TrimSpace(c.Query("provider"))),
		Outcome:  strings.TrimSpace(c.Query("outcome")),
		State:    strings.TrimSpace(c.Query("state")),
	}
	for key, dst := range map[string]*time.Time{"from": &q.From, "to": &q.To} {
		if raw := strings.TrimSpace(c.Query(key)); raw != "" {
			n, err := strconv.ParseInt(raw, 10, 64)
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": "invalid " + key + " (unix seconds)"})
				return
			}
			*dst = time.Unix(n, 0).UTC()
		}
	}
	q.BeforeID, _ = strconv.ParseInt(strings.TrimSpace(c.Query("before_id")), 10, 64)
	q.Limit, _ = strconv.Atoi(c.DefaultQuery("limit", "20"))
	if q.Limit <= 0 || q.Limit > 500 {
		q.Limit = 20
	}
	items, err := svc.History(c.Request.Context(), q)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if items == nil {
		items = []resets.Redemption{}
	}
	next := int64(0)
	if len(items) == q.Limit {
		next = items[len(items)-1].ID
	}
	c.JSON(http.StatusOK, gin.H{"enabled": true, "items": items, "next_before_id": next})
}

// tmxResetsBalanceHistory: GET /tmx/resets/balance-history?from&to&auth_id&provider&source&after_id&limit
// — every stored read of an account's banked credits, oldest first. from/to are unix seconds
// (default the last 24 hours); limit ≤ 10000 (default 1000). Pass next_after_id back as after_id
// for the following page; 0 on the last page.
func tmxResetsBalanceHistory(c *gin.Context) {
	svc := resets.Default()
	if svc == nil {
		c.JSON(http.StatusOK, gin.H{"enabled": false, "items": []any{}, "next_after_id": 0})
		return
	}
	from, to, ok := tmxRange(c)
	if !ok {
		return
	}
	q := resets.BalanceQuery{From: from, To: to, AuthID: strings.TrimSpace(c.Query("auth_id")),
		Provider: strings.ToLower(strings.TrimSpace(c.Query("provider"))), Source: strings.TrimSpace(c.Query("source"))}
	if raw := strings.TrimSpace(c.Query("after_id")); raw != "" {
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || n < 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid after_id"})
			return
		}
		q.AfterID = n
	}
	q.Limit, _ = strconv.Atoi(c.DefaultQuery("limit", "1000"))
	items, next, err := svc.BalanceHistory(c.Request.Context(), q)
	if errors.Is(err, resets.ErrNoBalanceHistory) {
		c.JSON(http.StatusOK, gin.H{"enabled": false, "items": []any{}, "next_after_id": 0})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"enabled": true, "items": items, "next_after_id": next, "stats": svc.BalanceStats()})
}

// redeemActor: optional caller label (e.g. "fleet-ui:vivin"), printable and bounded, plus the
// client address.
func redeemActor(raw, ip string) string {
	raw = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return -1
		}
		return r
	}, strings.TrimSpace(raw))
	if r := []rune(raw); len(r) > 64 {
		raw = string(r[:64])
	}
	if raw == "" {
		return ip
	}
	return raw + " @ " + ip
}

// tmxResetsRedeem: POST /tmx/resets/redeem {auth_id, credit_id, actor?} — spends one credit.
// 200 with the journaled claim (state settled or pending) whenever the claim was sent; 409 with
// {blocker} when the provider would refuse it or another redeem for the account is running.
// Safe to repeat: see wiki/reset-credits.md.
func tmxResetsRedeem(c *gin.Context) {
	svc := resets.Default()
	if svc == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "reset credits are off (TMX_RESETS)"})
		return
	}
	var body struct {
		AuthID   string `json:"auth_id"`
		CreditID string `json:"credit_id"`
		Actor    string `json:"actor"`
	}
	if err := c.ShouldBindJSON(&body); err != nil || strings.TrimSpace(body.AuthID) == "" || strings.TrimSpace(body.CreditID) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "body must be {\"auth_id\": ..., \"credit_id\": ..., \"actor\"?: ...}"})
		return
	}
	res, err := svc.Redeem(c.Request.Context(), body.AuthID, body.CreditID, redeemActor(body.Actor, c.ClientIP()))
	if tmxResetsError(c, err) {
		return
	}
	withPlans(res.Account)
	if res.Redemption.Outcome == resets.OutcomeReset || res.Redemption.Outcome == resets.OutcomeAlreadyRedeemed {
		// Refresh quota history too (throttled there; best effort).
		if q := quota.Default(); q != nil {
			go func(id string) { _, _, _ = q.PollNow(context.Background(), id) }(strings.TrimSpace(body.AuthID))
		}
	}
	c.JSON(http.StatusOK, res)
}

// tmxResetsError writes the HTTP error for err and reports whether it did.
func tmxResetsError(c *gin.Context, err error) bool {
	var blocked *resets.BlockedError
	switch {
	case err == nil:
		return false
	case errors.As(err, &blocked):
		c.JSON(http.StatusConflict, gin.H{"error": err.Error(), "blocker": blocked.Code})
	case errors.Is(err, resets.ErrBusy):
		c.JSON(http.StatusConflict, gin.H{"error": err.Error(), "blocker": "busy"})
	case errors.Is(err, resets.ErrUnknownAccount):
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
	case errors.Is(err, resets.ErrUnsupported):
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	case errors.Is(err, resets.ErrReadFailed), errors.Is(err, resets.ErrNoOrganization):
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
	default:
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
	}
	return true
}
