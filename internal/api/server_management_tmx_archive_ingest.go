package api

// Archive batch ingest routes (tokenmaxx fork): prompt history, session attachments and appended
// transcript bytes. Contract: ../wiki/archive-ingest.md (Prompt history / Attachments /
// Appending to session files). Each batch answers per item; see archive.BatchResult.HTTPStatus.

import (
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/archive"
)

const (
	tmxPromptBodyBytes     = 128 << 20
	tmxAttachmentBodyBytes = 128 << 20
	tmxAppendBodyBytes     = 96 << 20
	tmxFileStateBodyBytes  = 4 << 20
)

func registerTMXArchiveIngestRoutes(g *gin.RouterGroup) {
	g.PUT("/prompts", tmxArchive(tmxPromptIngest))
	g.GET("/prompts", tmxArchive(tmxPromptList))
	g.PUT("/attachments", tmxArchive(tmxAttachmentIngest))
	g.GET("/attachments", tmxArchive(tmxAttachmentList))
	g.GET("/files", tmxArchive(tmxFileGet))
	g.HEAD("/files", tmxArchive(tmxFileHead))
	g.POST("/files/state", tmxArchive(tmxFileStates))
	g.POST("/files/append", tmxArchive(tmxFileAppend))
	g.GET("/files/content", tmxArchive(tmxFileContent))
}

// tmxBindLimited decodes a JSON body of at most limit bytes; false (response written) on error.
func tmxBindLimited(c *gin.Context, limit int64, v any) bool {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, limit)
	if err := c.ShouldBindJSON(v); err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			c.JSON(http.StatusRequestEntityTooLarge, gin.H{"error": "body larger than " + strconv.FormatInt(limit>>20, 10) + " MiB; send smaller batches"})
			return false
		}
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return false
	}
	return true
}

func tmxBatchReply(c *gin.Context, res *archive.BatchResult, err error) {
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(res.HTTPStatus(), res)
}

// tmxPromptIngest: PUT /tmx/archive/prompts — lines of a prompt history file.
func tmxPromptIngest(c *gin.Context, a *archive.Archive) {
	var in archive.PromptBatch
	if !tmxBindLimited(c, tmxPromptBodyBytes, &in) {
		return
	}
	res, err := a.IngestPrompts(c.Request.Context(), in)
	tmxBatchReply(c, res, err)
}

func tmxUnixParam(c *gin.Context, name string) (time.Time, bool) {
	raw := strings.TrimSpace(c.Query(name))
	if raw == "" {
		return time.Time{}, true
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": name + " must be unix seconds"})
		return time.Time{}, false
	}
	return time.Unix(n, 0), true
}

// tmxPromptList: GET /tmx/archive/prompts?machine=&harness=&session_id=&since=&until=&limit=&cursor=&include=text
func tmxPromptList(c *gin.Context, a *archive.Archive) {
	since, ok := tmxUnixParam(c, "since")
	if !ok {
		return
	}
	until, ok := tmxUnixParam(c, "until")
	if !ok {
		return
	}
	limit, _ := strconv.Atoi(c.Query("limit"))
	items, next, err := a.ListPrompts(c.Request.Context(), archive.PromptQuery{Machine: c.Query("machine"),
		Harness: c.Query("harness"), SessionID: c.Query("session_id"), Since: since, Until: until, Limit: limit,
		Cursor: strings.TrimSpace(c.Query("cursor"))}, c.Query("include") == "text")
	tmxListReply(c, items, next, err)
}

func tmxListReply(c *gin.Context, items any, next string, err error) {
	switch {
	case errors.Is(err, archive.ErrBadCursor):
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	case err != nil:
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
	default:
		c.JSON(http.StatusOK, gin.H{"items": items, "next": next})
	}
}

// tmxAttachmentIngest: PUT /tmx/archive/attachments — files kept next to transcripts.
func tmxAttachmentIngest(c *gin.Context, a *archive.Archive) {
	var in archive.AttachmentBatch
	if !tmxBindLimited(c, tmxAttachmentBodyBytes, &in) {
		return
	}
	res, err := a.IngestAttachments(c.Request.Context(), in)
	tmxBatchReply(c, res, err)
}

// tmxAttachmentList: GET /tmx/archive/attachments?harness=&session_id=&machine=&path=&limit=&cursor=
func tmxAttachmentList(c *gin.Context, a *archive.Archive) {
	limit, _ := strconv.Atoi(c.Query("limit"))
	items, next, err := a.ListAttachments(c.Request.Context(), archive.AttachmentQuery{Harness: c.Query("harness"),
		SessionID: c.Query("session_id"), Machine: c.Query("machine"), Path: c.Query("path"), Limit: limit,
		Cursor: strings.TrimSpace(c.Query("cursor"))})
	tmxListReply(c, items, next, err)
}

func tmxFileRef(c *gin.Context) archive.FileRef {
	return archive.FileRef{Harness: c.Query("harness"), SessionID: c.Query("session_id"), Name: c.Query("name")}
}

func tmxFileError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, archive.ErrFileNotFound):
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
	case errors.Is(err, archive.ErrBadFileRef):
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	default:
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
	}
}

func tmxFileHeaders(c *gin.Context, st *archive.FileState) {
	c.Header("X-Tmx-File-Length", strconv.FormatInt(st.Length, 10))
	c.Header("X-Tmx-File-Sha256", st.SHA256)
	c.Header("X-Tmx-File-Generation", strconv.Itoa(st.Generation))
	c.Header("X-Tmx-File-Chunks", strconv.Itoa(st.Chunks))
}

// tmxFileGet: GET /tmx/archive/files?harness=&session_id=&name= — one file's state; without
// name, every file of the session ({items}).
func tmxFileGet(c *gin.Context, a *archive.Archive) {
	ref := tmxFileRef(c)
	if strings.TrimSpace(ref.Name) == "" {
		key := ref.SessionID
		if h := strings.TrimSpace(ref.Harness); h != "" {
			key = archive.SessionKey(h, strings.TrimSpace(ref.SessionID))
		}
		if !strings.Contains(key, ":") {
			c.JSON(http.StatusBadRequest, gin.H{"error": "harness and session_id are required"})
			return
		}
		items, err := a.ListFiles(c.Request.Context(), key)
		if err != nil {
			tmxFileError(c, err)
			return
		}
		c.JSON(http.StatusOK, gin.H{"items": items})
		return
	}
	st, err := a.FileStateOf(c.Request.Context(), ref)
	if err != nil {
		tmxFileError(c, err)
		return
	}
	tmxFileHeaders(c, st)
	c.JSON(http.StatusOK, st)
}

// tmxFileHead: HEAD /tmx/archive/files?harness=&session_id=&name= — the state in headers only.
func tmxFileHead(c *gin.Context, a *archive.Archive) {
	st, err := a.FileStateOf(c.Request.Context(), tmxFileRef(c))
	switch {
	case errors.Is(err, archive.ErrFileNotFound):
		c.Status(http.StatusNotFound)
	case errors.Is(err, archive.ErrBadFileRef):
		c.Status(http.StatusBadRequest)
	case err != nil:
		c.Status(http.StatusInternalServerError)
	default:
		tmxFileHeaders(c, st)
		c.Status(http.StatusOK)
	}
}

// tmxFileStates: POST /tmx/archive/files/state {"files": [{harness, session_id, name}]}.
func tmxFileStates(c *gin.Context, a *archive.Archive) {
	var in struct {
		Files []archive.FileRef `json:"files"`
	}
	if !tmxBindLimited(c, tmxFileStateBodyBytes, &in) {
		return
	}
	res, err := a.FileStates(c.Request.Context(), in.Files)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	status := http.StatusOK
	if res.Errors > 0 {
		status = http.StatusMultiStatus
	}
	c.JSON(status, res)
}

// tmxFileAppend: POST /tmx/archive/files/append — new bytes of growing session files.
func tmxFileAppend(c *gin.Context, a *archive.Archive) {
	var in archive.FileAppendBatch
	if !tmxBindLimited(c, tmxAppendBodyBytes, &in) {
		return
	}
	res, err := a.AppendFiles(c.Request.Context(), in)
	tmxBatchReply(c, res, err)
}

// tmxFileContent: GET /tmx/archive/files/content?harness=&session_id=&name=[&generation=] — the
// logical file (its chunks concatenated).
func tmxFileContent(c *gin.Context, a *archive.Archive) {
	gen := 0
	if raw := strings.TrimSpace(c.Query("generation")); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "generation must be a positive integer"})
			return
		}
		gen = n
	}
	started := false
	err := a.WriteFile(c.Request.Context(), tmxFileRef(c), gen, func(st *archive.FileState) {
		tmxFileHeaders(c, st)
		c.Header("Content-Length", strconv.FormatInt(st.Length, 10))
		c.Header("Content-Type", "application/octet-stream")
		c.Status(http.StatusOK)
		started = true
	}, c.Writer)
	if err != nil {
		if started {
			// Headers are gone; cut the body short so the client sees a length mismatch.
			_ = c.Error(err)
			c.Abort()
			return
		}
		tmxFileError(c, err)
	}
}
