package api

// tokenmaxx fork: read-only cooldown history (internal/tmx/cooldowns, table tmx.cooldown_events).

import (
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/cooldowns"
)

const (
	tmxCooldownDefaultRange = 7 * 24 * time.Hour
	tmxCooldownMaxRange     = 400 * 24 * time.Hour
)

func registerTMXCooldownRoutes(v8 *gin.RouterGroup) {
	v8.GET("/tmx/cooldowns/history", tmxCooldownHistory)
	v8.GET("/tmx/cooldowns/stats", tmxCooldownStats)
}

// tmxCooldownHistory: GET /tmx/cooldowns/history?from&to&auth_id&model&kind&source&limit&cursor.
// from/to are RFC3339 (or unix seconds); default the last 7 days. Events newest first, paged by
// cursor; windows (paired start/end, overlapping the range) on the first page only.
func tmxCooldownHistory(c *gin.Context) {
	to := time.Now().UTC()
	from := to.Add(-tmxCooldownDefaultRange)
	for key, dst := range map[string]*time.Time{"from": &from, "to": &to} {
		if raw := strings.TrimSpace(c.Query(key)); raw != "" {
			t, ok := parseTMXTime(raw)
			if !ok {
				c.JSON(http.StatusBadRequest, gin.H{"error": "invalid " + key + ": want RFC3339 or unix seconds"})
				return
			}
			*dst = t
		}
	}
	if raw := strings.TrimSpace(c.Query("from")); raw == "" && strings.TrimSpace(c.Query("to")) != "" {
		from = to.Add(-tmxCooldownDefaultRange)
	}
	if !from.Before(to) || to.Sub(from) > tmxCooldownMaxRange {
		c.JSON(http.StatusBadRequest, gin.H{"error": "from must be before to, at most 400 days apart"})
		return
	}
	limit := cooldowns.DefaultLimit
	if raw := strings.TrimSpace(c.Query("limit")); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 || n > cooldowns.MaxLimit {
			c.JSON(http.StatusBadRequest, gin.H{"error": "limit must be 1-" + strconv.Itoa(cooldowns.MaxLimit)})
			return
		}
		limit = n
	}
	q := cooldowns.Query{
		From:    from,
		To:      to,
		AuthID:  strings.TrimSpace(c.Query("auth_id")),
		Model:   strings.TrimSpace(c.Query("model")),
		Kinds:   tmxSplitList(c.Query("kind")),
		Sources: tmxSplitList(c.Query("source")),
		Cursor:  strings.TrimSpace(c.Query("cursor")),
		Limit:   limit,
	}
	store := cooldowns.Default()
	if store == nil {
		c.JSON(http.StatusOK, gin.H{"enabled": false, "events": []any{}, "windows": []any{}, "next_cursor": ""})
		return
	}
	page, err := store.History(c.Request.Context(), q)
	if errors.Is(err, cooldowns.ErrBadCursor) {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"enabled":           true,
		"from":              from.Format(time.RFC3339),
		"to":                to.Format(time.RFC3339),
		"events":            page.Events,
		"windows":           page.Windows,
		"windows_truncated": page.Truncated,
		"next_cursor":       page.NextCursor,
		"stats":             store.Stats(),
	})
}

// tmxCooldownStats: GET /tmx/cooldowns/stats — writer counters.
func tmxCooldownStats(c *gin.Context) {
	store := cooldowns.Default()
	if store == nil {
		c.JSON(http.StatusOK, gin.H{"enabled": false})
		return
	}
	c.JSON(http.StatusOK, gin.H{"enabled": true, "stats": store.Stats()})
}

func parseTMXTime(raw string) (time.Time, bool) {
	if t, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return t.UTC(), true
	}
	if n, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return time.Unix(n, 0).UTC(), true
	}
	return time.Time{}, false
}

func tmxSplitList(raw string) []string {
	var out []string
	for _, part := range strings.Split(raw, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}
