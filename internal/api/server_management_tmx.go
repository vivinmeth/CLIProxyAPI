package api

// tokenmaxx fork: management routes for the persistent request log (internal/tmx/requestlog).

import (
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/archive"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/quota"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/requestlog"
)

const tmxMaxSummaryRange = 400 * 24 * time.Hour

func registerTMXManagementRoutes(v8 *gin.RouterGroup, fetcher quota.Fetcher) {
	v8.GET("/tmx/requests/summary", tmxRequestSummary)
	v8.GET("/tmx/requests", tmxRequestList)
	v8.GET("/tmx/requests/rollup", tmxRequestRollup)
	v8.GET("/tmx/requests/sessions", tmxRequestSessions)
	registerTMXArchiveRoutes(v8)
	if store := requestlog.Default(); store != nil {
		quota.Start(store.DB(), store.Schema(), fetcher)
	}
	v8.GET("/tmx/quota/current", tmxQuotaCurrent)
	v8.GET("/tmx/quota/history", tmxQuotaHistory)
	v8.POST("/tmx/quota/poll", tmxQuotaPoll)
}

// tmxQuotaCurrent: GET /tmx/quota/current — latest reading of every window of every account.
func tmxQuotaCurrent(c *gin.Context) {
	svc := quota.Default()
	if svc == nil {
		c.JSON(http.StatusOK, gin.H{"enabled": false, "accounts": []any{}})
		return
	}
	c.JSON(http.StatusOK, gin.H{"enabled": true, "accounts": svc.Accounts()})
}

// tmxQuotaPoll: POST /tmx/quota/poll?auth_id=… — poll one account now and return its windows.
// Throttled (30 s per account, or while paused after a 429/401) returns the current state.
func tmxQuotaPoll(c *gin.Context) {
	svc := quota.Default()
	if svc == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "quota tracking is not running"})
		return
	}
	acc, throttled, err := svc.PollNow(c.Request.Context(), strings.TrimSpace(c.Query("auth_id")))
	switch {
	case errors.Is(err, quota.ErrUnknownAccount):
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
	case errors.Is(err, quota.ErrUnsupportedProvider):
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	case err != nil:
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
	default:
		c.JSON(http.StatusOK, gin.H{"account": acc, "throttled": throttled})
	}
}

// tmxQuotaHistory: GET /tmx/quota/history?auth_id&window&from&to&limit — snapshots oldest first
// (default last 7 days, limit 20000, max 100000).
func tmxQuotaHistory(c *gin.Context) {
	svc := quota.Default()
	if svc == nil {
		c.JSON(http.StatusOK, gin.H{"enabled": false, "snapshots": []any{}})
		return
	}
	to := time.Now().UTC()
	from := to.Add(-7 * 24 * time.Hour)
	for key, dst := range map[string]*time.Time{"from": &from, "to": &to} {
		if raw := strings.TrimSpace(c.Query(key)); raw != "" {
			n, err := strconv.ParseInt(raw, 10, 64)
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": "invalid " + key})
				return
			}
			*dst = time.Unix(n, 0).UTC()
		}
	}
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "20000"))
	if limit <= 0 || limit > 100000 {
		limit = 20000
	}
	rows, truncated, err := svc.History(c.Request.Context(), strings.TrimSpace(c.Query("auth_id")),
		strings.TrimSpace(c.Query("window")), from, to, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"enabled": true, "snapshots": rows, "truncated": truncated})
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

// tmxFilter reads the shared filter query parameters.
func tmxFilter(c *gin.Context) (requestlog.Filter, bool) {
	from, to, ok := tmxRange(c)
	if !ok {
		return requestlog.Filter{}, false
	}
	f := requestlog.Filter{
		From:            from,
		To:              to,
		IncludeShadowed: c.Query("include_shadowed") == "1" || c.Query("include_shadowed") == "true",
		Outcome:         strings.TrimSpace(c.Query("outcome")),
		Provider:        strings.TrimSpace(c.Query("provider")),
		AuthID:          strings.TrimSpace(c.Query("auth_id")),
		Model:           strings.TrimSpace(c.Query("model")),
		SessionID:       strings.TrimSpace(c.Query("session_id")),
		APIKeyHash:      strings.TrimSpace(c.Query("api_key_hash")),
		Machine:         strings.TrimSpace(c.Query("machine")),
		Harness:         strings.TrimSpace(c.Query("harness")),
		ClientMode:      strings.TrimSpace(c.Query("client_mode")),
	}
	if raw := strings.TrimSpace(c.Query("source")); raw != "" {
		f.Sources = strings.Split(raw, ",")
	}
	return f, true
}

func tmxRequestSummary(c *gin.Context) {
	store := requestlog.Default()
	if store == nil {
		c.JSON(http.StatusOK, gin.H{"enabled": false})
		return
	}
	f, ok := tmxFilter(c)
	if !ok {
		return
	}
	bucket := defaultBucket(f.To.Sub(f.From))
	if raw := strings.TrimSpace(c.Query("bucket")); raw != "" {
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || n < 60 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "bucket must be seconds >= 60"})
			return
		}
		bucket = time.Duration(n) * time.Second
		if f.To.Sub(f.From)/bucket > 2000 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "too many buckets"})
			return
		}
	}
	summary, err := store.Summarize(c.Request.Context(), f, bucket)
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
	f, ok := tmxFilter(c)
	if !ok {
		return
	}
	limit, _ := strconv.Atoi(strings.TrimSpace(c.Query("limit")))
	beforeID, _ := strconv.ParseInt(strings.TrimSpace(c.Query("before_id")), 10, 64)
	items, err := store.List(c.Request.Context(), f, beforeID, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	next := int64(0)
	if len(items) > 0 && len(items) == normalizedLimit(limit) {
		next = items[len(items)-1].ID
	}
	c.JSON(http.StatusOK, gin.H{"enabled": true, "items": items, "next_before_id": next})
}

// tmxRequestRollup: GET /tmx/requests/rollup?from&to&bucket=1h|1d&tz=Asia/Kolkata&dims=a,b
// plus the shared filters. Returns at most 50k cells; "truncated" tells the caller to narrow.
func tmxRequestRollup(c *gin.Context) {
	store := requestlog.Default()
	if store == nil {
		c.JSON(http.StatusOK, gin.H{"enabled": false, "rows": []any{}})
		return
	}
	f, ok := tmxFilter(c)
	if !ok {
		return
	}
	unit := map[string]string{"1h": "hour", "hour": "hour", "1d": "day", "day": "day"}[strings.TrimSpace(c.DefaultQuery("bucket", "1d"))]
	tz := strings.TrimSpace(c.DefaultQuery("tz", "UTC"))
	if _, err := time.LoadLocation(tz); err != nil || unit == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "bucket must be 1h or 1d and tz a valid IANA zone"})
		return
	}
	var dims []string
	if raw := strings.TrimSpace(c.Query("dims")); raw != "" {
		dims = strings.Split(raw, ",")
	}
	maxRows := 50000
	if n, errLimit := strconv.Atoi(strings.TrimSpace(c.Query("limit"))); errLimit == nil && n > 0 && n < maxRows {
		maxRows = n
	}
	rows, truncated, err := store.Rollup(c.Request.Context(), f, unit, tz, dims, maxRows)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"enabled": true, "bucket": unit, "tz": tz, "dims": dims,
		"rows": rows, "truncated": truncated})
}

// tmxRequestSessions: GET /tmx/requests/sessions?from&to&order=tokens|recent&limit plus the shared
// filters — per-session totals, largest first (limit ≤ 1000, default 100).
func tmxRequestSessions(c *gin.Context) {
	store := requestlog.Default()
	if store == nil {
		c.JSON(http.StatusOK, gin.H{"enabled": false, "sessions": []any{}})
		return
	}
	f, ok := tmxFilter(c)
	if !ok {
		return
	}
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "100"))
	sessions, err := store.SessionTotals(c.Request.Context(), f, strings.TrimSpace(c.Query("order")), limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"enabled": true, "sessions": sessions})
}

func normalizedLimit(n int) int {
	if n <= 0 || n > 500 {
		return 100
	}
	return n
}

// ---- archive (internal/tmx/archive): full exchange capture + session ingest ----

const tmxMaxBlobBytes = 512 << 20

func registerTMXArchiveRoutes(v8 *gin.RouterGroup) {
	g := v8.Group("/tmx/archive")
	g.GET("/status", tmxArchive(func(c *gin.Context, a *archive.Archive) {
		c.JSON(http.StatusOK, gin.H{"enabled": true, "stats": a.Stats()})
	}))
	g.GET("/storage", tmxArchive(tmxArchiveStorage))
	g.POST("/blobs/missing", tmxArchive(tmxBlobsMissing))
	g.PUT("/blobs/:hash", tmxArchive(tmxBlobPut))
	g.GET("/blobs/:hash", tmxArchive(tmxBlobGet))
	g.PUT("/sessions", tmxArchive(tmxSessionIngest))
	g.GET("/sessions", tmxArchive(tmxSessionList))
	g.GET("/sessions/*id", tmxArchive(tmxSessionGet))
	g.DELETE("/sessions/*id", tmxArchive(tmxSessionDelete))
	g.POST("/usage/rebuild", tmxArchive(tmxUsageRebuild))
	g.GET("/exchanges", tmxArchive(tmxExchangeList))
	g.GET("/exchanges/:trace", tmxArchive(tmxExchangeGet))
}

func tmxArchive(h func(*gin.Context, *archive.Archive)) gin.HandlerFunc {
	return func(c *gin.Context) {
		a := archive.Default()
		if a == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"enabled": false, "error": "archive is off (TMX_ARCHIVE)"})
			return
		}
		h(c, a)
	}
}

func tmxBlobsMissing(c *gin.Context, a *archive.Archive) {
	var body struct {
		Hashes []string `json:"hashes"`
	}
	if err := c.ShouldBindJSON(&body); err != nil || len(body.Hashes) > 10000 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "body must be {\"hashes\": [sha256...]} (max 10000)"})
		return
	}
	for _, h := range body.Hashes {
		if !archive.ValidHash(h) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid hash " + h})
			return
		}
	}
	missing, err := a.Blobs().Missing(c.Request.Context(), body.Hashes)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"missing": missing})
}

func tmxBlobPut(c *gin.Context, a *archive.Archive) {
	hash := strings.ToLower(c.Param("hash"))
	if !archive.ValidHash(hash) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "path must be the sha256 hex of the body"})
		return
	}
	data, err := io.ReadAll(io.LimitReader(c.Request.Body, tmxMaxBlobBytes+1))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if len(data) > tmxMaxBlobBytes {
		c.JSON(http.StatusRequestEntityTooLarge, gin.H{"error": "blob larger than 512 MiB; split it"})
		return
	}
	if got := archive.HashOf(data); got != hash {
		c.JSON(http.StatusBadRequest, gin.H{"error": "sha256 mismatch", "got": got})
		return
	}
	kind := strings.TrimSpace(c.GetHeader("X-Tmx-Blob-Kind"))
	if kind == "" {
		kind = "import.source"
	}
	if _, err = a.Blobs().Put(c.Request.Context(), data, c.ContentType(), kind); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"hash": hash, "size": len(data)})
}

func tmxBlobGet(c *gin.Context, a *archive.Archive) {
	hash := strings.ToLower(c.Param("hash"))
	data, err := a.Blobs().Get(c.Request.Context(), hash)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	mt := a.Blobs().MediaType(c.Request.Context(), hash)
	if mt == "" {
		mt = "application/octet-stream"
	}
	c.Data(http.StatusOK, mt, data)
}

func tmxSessionIngest(c *gin.Context, a *archive.Archive) {
	var in archive.SessionIngest
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	res, err := a.IngestSession(c.Request.Context(), in)
	if err != nil {
		status := http.StatusBadRequest
		if res != nil && len(res.MissingBlobs) > 0 {
			status = http.StatusConflict
		}
		c.JSON(status, gin.H{"error": err.Error(), "result": res})
		return
	}
	c.JSON(http.StatusOK, res)
}

func tmxSessionList(c *gin.Context, a *archive.Archive) {
	since := time.Unix(0, 0)
	if raw := strings.TrimSpace(c.Query("since")); raw != "" {
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "since must be unix seconds"})
			return
		}
		since = time.Unix(n, 0)
	}
	limit, _ := strconv.Atoi(c.Query("limit"))
	items, err := a.ListSessions(c.Request.Context(), c.Query("harness"), c.Query("machine"), since, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": items})
}

func tmxSessionGet(c *gin.Context, a *archive.Archive) {
	id := strings.TrimPrefix(c.Param("id"), "/")
	out, err := a.GetSession(c.Request.Context(), id)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, out)
}

// tmxArchiveStorage: GET /tmx/archive/storage[?refresh=1] — objects and bytes in the archive
// bucket per environment folder, the bucket's storage cost at list price, and this environment's
// cost estimate. Listing is cached for an hour (refresh: at most once a minute).
func tmxArchiveStorage(c *gin.Context, a *archive.Archive) {
	usage, err := a.Storage(c.Request.Context(), c.Query("refresh") == "1" || c.Query("refresh") == "true")
	cost, errCost := a.Cost(c.Request.Context())
	if errCost != nil {
		cost = nil
	}
	switch {
	case errors.Is(err, archive.ErrNoR2):
		c.JSON(http.StatusOK, gin.H{"enabled": true, "r2_configured": false, "cost_estimate": cost})
	case err != nil:
		c.JSON(http.StatusBadGateway, gin.H{"error": "bucket listing failed: " + err.Error(), "cost_estimate": cost})
	default:
		c.JSON(http.StatusOK, gin.H{"enabled": true, "r2_configured": true, "storage": usage, "cost_estimate": cost})
	}
}

// tmxUsageRebuild: POST /tmx/archive/usage/rebuild?harness=&dry_run=1 — restore imported usage
// rows missing from tmx.requests from the archive's stored messages. Never overwrites rows.
func tmxUsageRebuild(c *gin.Context, a *archive.Archive) {
	dry := c.Query("dry_run") == "1" || c.Query("dry_run") == "true"
	res, err := a.RebuildUsage(c.Request.Context(), strings.TrimSpace(c.Query("harness")), dry)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error(), "result": res})
		return
	}
	c.JSON(http.StatusOK, res)
}

// tmxSessionDelete: DELETE /tmx/archive/sessions/<harness>:<id> — removes an imported session,
// its messages, source links and imported usage rows (blobs stay).
func tmxSessionDelete(c *gin.Context, a *archive.Archive) {
	id := strings.TrimPrefix(c.Param("id"), "/")
	res, err := a.DeleteSession(c.Request.Context(), id)
	switch {
	case errors.Is(err, archive.ErrSessionNotFound):
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
	case err != nil:
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error(), "result": res})
	default:
		c.JSON(http.StatusOK, res)
	}
}

func tmxExchangeList(c *gin.Context, a *archive.Archive) {
	from, to, ok := tmxRange(c)
	if !ok {
		return
	}
	limit, _ := strconv.Atoi(c.Query("limit"))
	items, err := a.ListExchanges(c.Request.Context(), c.Query("session_id"), from, to, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": items})
}

func tmxExchangeGet(c *gin.Context, a *archive.Archive) {
	out, err := a.GetExchange(c.Request.Context(), c.Param("trace"))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, out)
}
