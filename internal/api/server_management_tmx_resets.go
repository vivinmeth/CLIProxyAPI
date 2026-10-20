package api

// tokenmaxx fork: management routes for banked reset credits (internal/tmx/resets).

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"

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
	v8.POST("/tmx/resets/redeem", tmxResetsRedeem)
}

// tmxResetsList: GET /tmx/resets[?refresh=1&history=N] — every Claude/Codex account's banked
// credits (soonest expiry first), the windows they clear, unsettled claims and recent history.
func tmxResetsList(c *gin.Context) {
	svc := resets.Default()
	if svc == nil {
		c.JSON(http.StatusOK, gin.H{"enabled": false, "accounts": []any{}, "history": []any{}})
		return
	}
	refresh := c.Query("refresh") == "1" || c.Query("refresh") == "true"
	accounts := svc.Accounts(c.Request.Context(), refresh)
	limit, _ := strconv.Atoi(c.DefaultQuery("history", "20"))
	history, err := svc.History(c.Request.Context(), limit)
	if err != nil {
		history = nil
	}
	if history == nil {
		history = []resets.Redemption{}
	}
	c.JSON(http.StatusOK, gin.H{"enabled": true, "accounts": accounts, "history": history})
}

// tmxResetsRedeem: POST /tmx/resets/redeem {auth_id, credit_id} — spends one credit. 200 with the
// journaled claim (state settled or pending) whenever the claim was sent; 409 when the provider
// would refuse it (blocker) or another redeem for the account is running.
func tmxResetsRedeem(c *gin.Context) {
	svc := resets.Default()
	if svc == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "reset credits are off (TMX_RESETS)"})
		return
	}
	var body struct {
		AuthID   string `json:"auth_id"`
		CreditID string `json:"credit_id"`
	}
	if err := c.ShouldBindJSON(&body); err != nil || strings.TrimSpace(body.AuthID) == "" || strings.TrimSpace(body.CreditID) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "body must be {\"auth_id\": ..., \"credit_id\": ...}"})
		return
	}
	res, err := svc.Redeem(c.Request.Context(), body.AuthID, body.CreditID, c.ClientIP())
	var blocked *resets.BlockedError
	switch {
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
	case err != nil:
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
	default:
		if res.Redemption.Outcome == resets.OutcomeReset || res.Redemption.Outcome == resets.OutcomeAlreadyRedeemed {
			// Refresh quota history too (throttled there; best effort).
			if q := quota.Default(); q != nil {
				go func(id string) { _, _, _ = q.PollNow(context.Background(), id) }(body.AuthID)
			}
		}
		c.JSON(http.StatusOK, res)
	}
}
