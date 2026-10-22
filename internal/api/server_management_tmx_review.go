package api

// tokenmaxx fork: GitHub PR ingest (archive) and the review-verdict store (analytics schema
// review) — wiki/archive-ingest.md → Pull requests, wiki/analytics.md → Review verdicts.

import (
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/archive"
)

func registerTMXReviewRoutes(v8 *gin.RouterGroup) {
	g := v8.Group("/tmx/archive")
	g.PUT("/pull-requests", tmxArchive(tmxPullRequestIngest))
	g.GET("/pull-requests", tmxArchive(tmxPullRequestList))
	g.GET("/pull-requests/:owner/:name/:number", tmxArchive(tmxPullRequestGet))

	r := v8.Group("/tmx/review")
	r.GET("/status", tmxArchive(tmxReviewStatus))
	r.POST("/reviews", tmxArchive(tmxReviewRecord))
	r.POST("/verdicts", tmxArchive(tmxVerdictRecord))
	r.GET("/findings", tmxArchive(tmxReviewFindings))
	r.GET("/tally", tmxArchive(tmxReviewTally))
}

// batchStatus: 200 when every item went through, 207 when some failed (see results[].error).
func batchStatus(errs int) int {
	if errs > 0 {
		return http.StatusMultiStatus
	}
	return http.StatusOK
}

// tmxPullRequestIngest: PUT /tmx/archive/pull-requests — store GitHub PRs (raw documents
// verbatim) with per-item results.
func tmxPullRequestIngest(c *gin.Context, a *archive.Archive) {
	var in archive.PullRequestBatch
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	res, err := a.IngestPullRequests(c.Request.Context(), in)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(batchStatus(res.Errors), res)
}

func tmxPullRequestList(c *gin.Context, a *archive.Archive) {
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
	items, next, err := a.ListPullRequests(c.Request.Context(), archive.PullRequestQuery{Repo: c.Query("repo"),
		Outcome: c.Query("outcome"), Machine: c.Query("machine"), Since: since, Limit: limit,
		Cursor: strings.TrimSpace(c.Query("cursor"))})
	switch {
	case errors.Is(err, archive.ErrBadCursor):
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	case err != nil:
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
	default:
		c.JSON(http.StatusOK, gin.H{"items": items, "next": next})
	}
}

func tmxPullRequestGet(c *gin.Context, a *archive.Archive) {
	n, err := strconv.ParseInt(c.Param("number"), 10, 64)
	if err != nil || n <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "number must be a positive integer"})
		return
	}
	out, err := a.GetPullRequest(c.Request.Context(), archive.PullRequestKey(c.Param("owner")+"/"+c.Param("name"), n))
	switch {
	case errors.Is(err, archive.ErrPullRequestNotFound):
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
	case err != nil:
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
	default:
		c.JSON(http.StatusOK, out)
	}
}

func tmxReviewError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, archive.ErrBatchTooLarge):
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	case errors.Is(err, archive.ErrAnalyticsOff):
		tmxAnalyticsError(c, err)
	default:
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": err.Error()})
	}
}

// tmxReviewStatus: GET /tmx/review/status — schema / role setup (setup_sql when agents_rw is
// missing) and row counts.
func tmxReviewStatus(c *gin.Context, a *archive.Archive) {
	st, err := a.ReviewStatus(c.Request.Context())
	if err != nil {
		tmxAnalyticsError(c, err)
		return
	}
	c.JSON(http.StatusOK, st)
}

// tmxReviewRecord: POST /tmx/review/reviews {"reviews": [...]} — reviewer runs with findings.
func tmxReviewRecord(c *gin.Context, a *archive.Archive) {
	var in struct {
		Reviews []archive.ReviewIngest `json:"reviews"`
	}
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	res, err := a.RecordReviews(c.Request.Context(), in.Reviews)
	if err != nil {
		tmxReviewError(c, err)
		return
	}
	c.JSON(batchStatus(res.Errors), res)
}

// tmxVerdictRecord: POST /tmx/review/verdicts {"verdicts": [...]} — append verdicts.
func tmxVerdictRecord(c *gin.Context, a *archive.Archive) {
	var in struct {
		Verdicts []archive.VerdictIngest `json:"verdicts"`
	}
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	res, err := a.RecordVerdicts(c.Request.Context(), in.Verdicts)
	if err != nil {
		tmxReviewError(c, err)
		return
	}
	c.JSON(batchStatus(res.Errors), res)
}

// tmxReviewFindings: GET /tmx/review/findings?review_id=&model=&repo=&pr=&verdict=(valid|…|none)
// &after=&limit= — findings with their review and latest verdict, by finding_id; pass the last
// finding_id as after for the next page.
func tmxReviewFindings(c *gin.Context, a *archive.Archive) {
	q := archive.FindingQuery{Model: c.Query("model"), Repo: c.Query("repo"), Verdict: c.Query("verdict")}
	for key, dst := range map[string]*int64{"review_id": &q.ReviewID, "pr": &q.PRNumber, "after": &q.AfterID} {
		if raw := strings.TrimSpace(c.Query(key)); raw != "" {
			n, err := strconv.ParseInt(raw, 10, 64)
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": key + " must be an integer"})
				return
			}
			*dst = n
		}
	}
	q.Limit, _ = strconv.Atoi(c.Query("limit"))
	items, err := a.ReviewFindings(c.Request.Context(), q)
	if err != nil {
		tmxReviewError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": items})
}

// tmxReviewTally: GET /tmx/review/tally?model=&harness=&repo=&since=(unix s) — per reviewer
// model: reviews, findings, judged, valid / invalid / dup / unclear, unique_valid, precision.
func tmxReviewTally(c *gin.Context, a *archive.Archive) {
	q := archive.TallyQuery{Model: c.Query("model"), Harness: c.Query("harness"), Repo: c.Query("repo"),
		Since: time.Unix(0, 0)}
	if raw := strings.TrimSpace(c.Query("since")); raw != "" {
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "since must be unix seconds"})
			return
		}
		q.Since = time.Unix(n, 0)
	}
	rows, err := a.ReviewTally(c.Request.Context(), q)
	if err != nil {
		tmxReviewError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"models": rows})
}
