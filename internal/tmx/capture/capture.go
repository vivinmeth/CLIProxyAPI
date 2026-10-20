// Package capture is the tokenmaxx fork's switch and hand-off point for full exchange capture.
//
// It is deliberately a leaf package (no project imports) so upstream code paths can consult
// Enabled() and hand over an Exchange without import cycles. The archive package registers the
// sink that actually stores exchanges.
package capture

import (
	"sync/atomic"
	"time"
)

var (
	enabled atomic.Bool
	sink    atomic.Pointer[func(*Exchange)]
)

// Enabled reports whether full capture is on. When it is, upstream request/response bodies are
// recorded the same way `request-log: true` would record them, independent of that setting.
func Enabled() bool { return enabled.Load() }

// SetSink installs the consumer and turns capture on (nil turns it off).
func SetSink(fn func(*Exchange)) {
	if fn == nil {
		enabled.Store(false)
		sink.Store(nil)
		return
	}
	sink.Store(&fn)
	enabled.Store(true)
}

// Submit hands a finished exchange to the sink. It must not block; the sink queues.
func Submit(ex *Exchange) {
	if ex == nil {
		return
	}
	if fn := sink.Load(); fn != nil {
		(*fn)(ex)
	}
}

// ChunkMark records when a response chunk was written to the client and how big it was.
type ChunkMark struct {
	OffsetMs int64 `json:"t"`
	Size     int   `json:"n"`
}

// Exchange is one proxied HTTP exchange (one WebSocket connection counts as one exchange).
// All byte slices are owned by the exchange; producers must pass copies.
type Exchange struct {
	TraceID         string
	SessionID       string
	URL             string
	Method          string
	StartedAt       time.Time
	FinishedAt      time.Time
	FirstChunkAt    time.Time
	Status          int
	Streaming       bool
	RequestHeaders  map[string][]string
	RequestBody     []byte
	ResponseHeaders map[string][]string
	ResponseBody    []byte
	Chunks          []ChunkMark
	// Upstream side, as recorded by the executors.
	APIRequest           []byte
	APIResponse          []byte
	APIResponseAt        time.Time
	WebsocketTimeline    []byte
	APIWebsocketTimeline []byte
	Errors               []string
}
