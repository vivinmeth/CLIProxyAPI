package middleware

// tokenmaxx fork: full exchange capture (internal/tmx/capture -> internal/tmx/archive).
// The upstream ResponseWriterWrapper only buffers what request logging needs and drops streamed
// chunks under backpressure; capture keeps its own lossless copy of every byte sent to the
// client, with per-chunk timing, and submits the whole exchange at Finalize.

import (
	"bytes"
	"fmt"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/interfaces"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/logging"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/capture"
)

type tmxCapture struct {
	body   bytes.Buffer
	chunks []capture.ChunkMark
	first  time.Time
}

// tmxTee records a chunk written to the client. Called after the client write.
func (w *ResponseWriterWrapper) tmxTee(data []byte) {
	if len(data) == 0 || !capture.Enabled() {
		return
	}
	if w.tmx == nil {
		w.tmx = &tmxCapture{}
	}
	now := time.Now()
	if w.tmx.first.IsZero() {
		w.tmx.first = now
	}
	var start time.Time
	if w.requestInfo != nil {
		start = w.requestInfo.Timestamp
	}
	offset := int64(0)
	if !start.IsZero() {
		offset = now.Sub(start).Milliseconds()
	}
	w.tmx.body.Write(data)
	w.tmx.chunks = append(w.tmx.chunks, capture.ChunkMark{OffsetMs: offset, Size: len(data)})
}

// tmxSubmit hands the finished exchange to the archive. It runs at the start of Finalize, while
// the gin context and every spooled source are still available, and copies what it needs.
func (w *ResponseWriterWrapper) tmxSubmit(c *gin.Context, status int) {
	if c == nil || !capture.Enabled() || w.requestInfo == nil {
		return
	}
	ex := &capture.Exchange{
		TraceID:         w.requestInfo.RequestID,
		URL:             w.requestInfo.URL,
		Method:          w.requestInfo.Method,
		StartedAt:       w.requestInfo.Timestamp,
		FinishedAt:      time.Now(),
		Status:          status,
		Streaming:       w.isStreaming,
		RequestHeaders:  w.requestInfo.Headers,
		RequestBody:     bytes.Clone(w.extractRequestBody(c)),
		ResponseHeaders: w.cloneHeaders(),
		APIResponseAt:   w.extractAPIResponseTimestamp(c),
	}
	if c.Request != nil {
		ex.SessionID = logging.GetClientRequestMetadata(c.Request.Context()).SessionID
	}
	if override := extractBodyOverride(c, responseBodyOverrideContextKey); len(override) > 0 {
		ex.ResponseBody = bytes.Clone(override)
	} else if w.tmx != nil {
		ex.ResponseBody = bytes.Clone(w.tmx.body.Bytes())
	}
	if w.tmx != nil {
		ex.Chunks = w.tmx.chunks
		ex.FirstChunkAt = w.tmx.first
	}
	ex.APIRequest = w.extractAPIRequest(c)
	if len(ex.APIRequest) == 0 {
		ex.APIRequest = w.extractDeferredAPIRequest(c)
	}
	ex.APIRequest = withSource(ex.APIRequest, w.extractAPIRequestSource(c))
	ex.APIResponse = withSource(w.extractAPIResponse(c), w.extractAPIResponseSource(c))
	ex.WebsocketTimeline = withSource(w.extractWebsocketTimeline(c), w.extractWebsocketTimelineSource(c))
	ex.APIWebsocketTimeline = withSource(w.extractAPIWebsocketTimeline(c), w.extractAPIWebsocketTimelineSource(c))
	if raw, ok := c.Get("API_RESPONSE_ERROR"); ok {
		if errs, okErrs := raw.([]*interfaces.ErrorMessage); okErrs {
			for _, e := range errs {
				if e == nil {
					continue
				}
				msg := ""
				if e.Error != nil {
					msg = e.Error.Error()
				}
				ex.Errors = append(ex.Errors, fmt.Sprintf("%d: %s", e.StatusCode, msg))
			}
		}
	}
	capture.Submit(ex)
}

// withSource appends a spooled source's bytes (without consuming or deleting it) to payload.
func withSource(payload []byte, source *logging.FileBodySource) []byte {
	out := bytes.Clone(payload)
	if source == nil || !source.HasPayload() {
		return out
	}
	data, err := source.Bytes()
	if err != nil {
		return out
	}
	return append(out, data...)
}
