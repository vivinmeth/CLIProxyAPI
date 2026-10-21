package middleware

// tokenmaxx fork: full exchange capture (internal/tmx/capture -> internal/tmx/archive).
// The upstream ResponseWriterWrapper only buffers what request logging needs and drops streamed
// chunks under backpressure; capture keeps its own lossless copy of every byte sent to the
// client, with per-chunk timing, and submits the whole exchange at Finalize.

import (
	"fmt"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/interfaces"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/logging"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/capture"
)

type tmxCapture struct {
	group      *capture.Group
	body       *capture.Body
	chunks     *capture.Body
	chunkCount int
	first      time.Time
}

// tmxTee records a chunk written to the client. Called after the client write.
func (w *ResponseWriterWrapper) tmxTee(data []byte) {
	if len(data) == 0 || !capture.Enabled() {
		return
	}
	if w.tmx == nil {
		group := &capture.Group{}
		w.tmx = &tmxCapture{group: group, body: group.NewBody(), chunks: group.NewBody()}
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
	_, _ = w.tmx.body.Write(data)
	var mark [96]byte
	p := mark[:0]
	if w.tmx.chunkCount == 0 {
		p = append(p, '[')
	} else {
		p = append(p, ',')
	}
	p = append(p, `{"t":`...)
	p = strconv.AppendInt(p, offset, 10)
	p = append(p, `,"n":`...)
	p = strconv.AppendInt(p, int64(len(data)), 10)
	p = append(p, '}')
	_, _ = w.tmx.chunks.Write(p)
	w.tmx.chunkCount++
}

// tmxSubmit hands the finished exchange to the archive. It runs at the start of Finalize, while
// the gin context and every spooled source are still available, and copies what it needs.
func (w *ResponseWriterWrapper) tmxSubmit(c *gin.Context, status int) {
	if c == nil || !capture.Enabled() || w.requestInfo == nil {
		if w.tmx != nil {
			w.tmx.body.Release()
			w.tmx.chunks.Release()
		}
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
		Bodies:          map[string]*capture.Body{},
		ResponseHeaders: w.cloneHeaders(),
		APIResponseAt:   w.extractAPIResponseTimestamp(c),
	}
	if c.Request != nil {
		ex.SessionID = logging.GetClientRequestMetadata(c.Request.Context()).SessionID
	}
	group := &capture.Group{}
	if w.tmx != nil {
		group = w.tmx.group
	}
	ex.Bodies["request"] = capture.CaptureInGroup(w.extractRequestBody(c), nil, group)
	if override := extractBodyOverride(c, responseBodyOverrideContextKey); len(override) > 0 {
		ex.Bodies["response"] = capture.CaptureInGroup(override, nil, group)
		if w.tmx != nil {
			w.tmx.body.Release()
		}
	} else if w.tmx != nil {
		ex.Bodies["response"] = w.tmx.body
	}
	if w.tmx != nil {
		if w.tmx.chunkCount > 0 {
			_, _ = w.tmx.chunks.Write([]byte("]"))
			ex.Bodies["chunks"] = w.tmx.chunks
		} else {
			w.tmx.chunks.Release()
		}
		ex.FirstChunkAt = w.tmx.first
	}
	request := w.extractAPIRequest(c)
	if len(request) == 0 {
		request = w.extractDeferredAPIRequest(c)
	}
	ex.Bodies["upstream.request"] = capture.CaptureInGroup(request, w.extractAPIRequestSource(c), group)
	ex.Bodies["upstream.response"] = capture.CaptureInGroup(w.extractAPIResponse(c), w.extractAPIResponseSource(c), group)
	ex.Bodies["ws"] = capture.CaptureInGroup(w.extractWebsocketTimeline(c), w.extractWebsocketTimelineSource(c), group)
	ex.Bodies["upstream.ws"] = capture.CaptureInGroup(w.extractAPIWebsocketTimeline(c), w.extractAPIWebsocketTimelineSource(c), group)
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
	if ex.CaptureError() != nil {
		ex.Release()
		return
	}
	capture.Submit(ex)
}
