package api

// tokenmaxx fork: management routes for the persistent request log (internal/tmx/requestlog).

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/requestlog"
)

const tmxMaxSummaryRange = 400 * 24 * time.Hour

func registerTMXManagementRoutes(v8 *gin.RouterGroup) {
	v8.GET("/tmx/requests/summary", tmxRequestSummary)
	v8.GET("/tmx/requests", tmxRequestList)
}

// tmxRange parses from/to (unix seconds) with a default of the last 24 hours.
func tmxRange(c *gin.Context) (time.Time, time.Time, bool) {
	now := time.Now().UTC()
	to := now
	from := now.Add(-24 * time.Hour)
	if raw := strings.TrimSpace(c.Query("to")); raw != "" {
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid to"})
			return from, to, false
		}
		to = time.Unix(n, 0).UTC()
	}
	if raw := strings.TrimSpace(c.Query("from")); raw != "" {
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid from"})
			return from, to, false
		}
		from = time.Unix(n, 0).UTC()
	}
	if !from.Before(to) || to.Sub(from) > tmxMaxSummaryRange {
		c.JSON(http.StatusBadRequest, gin.H{"error": "from must be before to, at most 400 days apart"})
		return from, to, false
	}
	return from, to, true
}

// defaultBucket picks a bucket so a range renders as roughly 60-150 points.
func defaultBucket(span time.Duration) time.Duration {
	for _, b := range []time.Duration{time.Minute, 5 * time.Minute, 10 * time.Minute,
		30 * time.Minute, time.Hour, 3 * time.Hour, 6 * time.Hour, 24 * time.Hour} {
		if span/b <= 150 {
			return b
		}
	}
	return 7 * 24 * time.Hour
}

func tmxRequestSummary(c *gin.Context) {
	store := requestlog.Default()
	if store == nil {
		c.JSON(http.StatusOK, gin.H{"enabled": false})
		return
	}
	from, to, ok := tmxRange(c)
	if !ok {
		return
	}
	bucket := defaultBucket(to.Sub(from))
	if raw := strings.TrimSpace(c.Query("bucket")); raw != "" {
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || n < 60 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "bucket must be seconds >= 60"})
			return
		}
		bucket = time.Duration(n) * time.Second
		if to.Sub(from)/bucket > 2000 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "too many buckets"})
			return
		}
	}
	summary, err := store.Summarize(c.Request.Context(), from, to, bucket)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"enabled": true, "summary": summary})
}

func tmxRequestList(c *gin.Context) {
	store := requestlog.Default()
	if store == nil {
		c.JSON(http.StatusOK, gin.H{"enabled": false, "items": []any{}})
		return
	}
	from, to, ok := tmxRange(c)
	if !ok {
		return
	}
	f := requestlog.ListFilter{
		From:      from,
		To:        to,
		Outcome:   strings.TrimSpace(c.Query("outcome")),
		Provider:  strings.TrimSpace(c.Query("provider")),
		AuthID:    strings.TrimSpace(c.Query("auth_id")),
		Model:     strings.TrimSpace(c.Query("model")),
		SessionID: strings.TrimSpace(c.Query("session_id")),
	}
	if raw := strings.TrimSpace(c.Query("limit")); raw != "" {
		f.Limit, _ = strconv.Atoi(raw)
	}
	if raw := strings.TrimSpace(c.Query("before_id")); raw != "" {
		f.BeforeID, _ = strconv.ParseInt(raw, 10, 64)
	}
	items, err := store.List(c.Request.Context(), f)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	next := int64(0)
	if len(items) > 0 && len(items) == normalizedLimit(f.Limit) {
		next = items[len(items)-1].ID
	}
	c.JSON(http.StatusOK, gin.H{"enabled": true, "items": items, "next_before_id": next})
}

func normalizedLimit(n int) int {
	if n <= 0 || n > 500 {
		return 100
	}
	return n
}
