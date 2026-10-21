package executor

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v8/sdk/translator"
)

// runClaudeStreamWithInStreamError streams message_start, one text delta and then an SSE error
// event, and returns the stream's terminal error.
func runClaudeStreamWithInStreamError(t *testing.T, errorType string, headers http.Header, format sdktranslator.Format) error {
	t.Helper()
	stream := "event: message_start\n" +
		`data: {"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","content":[],"model":"claude-opus-5","stop_reason":null,"usage":{"input_tokens":10,"output_tokens":1}}}` + "\n\n" +
		"event: content_block_start\n" +
		`data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}` + "\n\n" +
		"event: content_block_delta\n" +
		`data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"partial"}}` + "\n\n" +
		"event: error\n" +
		`data: {"type":"error","error":{"type":"` + errorType + `","message":"synthetic in-stream error"}}` + "\n\n"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		for key, values := range headers {
			for _, value := range values {
				w.Header().Add(key, value)
			}
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(stream))
	}))
	defer server.Close()

	exec := NewClaudeExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{"api_key": "key-123", "base_url": server.URL}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := exec.ExecuteStream(ctx, auth, cliproxyexecutor.Request{
		Model:   "claude-opus-5",
		Payload: []byte(`{"model":"claude-opus-5","messages":[{"role":"user","content":"hi"}],"stream":true}`),
	}, cliproxyexecutor.Options{SourceFormat: format, ResponseFormat: format, Stream: true})
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}
	var streamErr error
	sawPayload := false
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			streamErr = chunk.Err
			continue
		}
		if len(chunk.Payload) > 0 {
			sawPayload = true
		}
	}
	if !sawPayload {
		t.Fatal("the partial output before the error must still be delivered")
	}
	if streamErr == nil {
		t.Fatal("an SSE error event must end the stream with an error")
	}
	return streamErr
}

func TestClaudeExecutorInStreamErrorClassification(t *testing.T) {
	for _, format := range []sdktranslator.Format{sdktranslator.FromString("claude"), sdktranslator.FormatOpenAIResponse} {
		for _, tc := range []struct {
			errorType string
			status    int
		}{
			{"rate_limit_error", http.StatusTooManyRequests},
			{"overloaded_error", 529},
			{"authentication_error", http.StatusUnauthorized},
			{"permission_error", http.StatusForbidden},
			{"api_error", http.StatusBadGateway},
		} {
			t.Run(string(format)+"/"+tc.errorType, func(t *testing.T) {
				err := runClaudeStreamWithInStreamError(t, tc.errorType, nil, format)
				status, ok := err.(interface{ StatusCode() int })
				if !ok || status.StatusCode() != tc.status {
					t.Fatalf("%s classified as %v; want status %d", tc.errorType, err, tc.status)
				}
			})
		}
	}
}

func TestClaudeExecutorInStreamRateLimitUsesStreamStartHeaders(t *testing.T) {
	reset := time.Now().Add(2 * time.Hour)
	headers := http.Header{}
	headers.Set("Anthropic-Ratelimit-Unified-Status", "rejected")
	headers.Set("Anthropic-Ratelimit-Unified-5h-Status", "rejected")
	headers.Set("Anthropic-Ratelimit-Unified-5h-Reset", strconv.FormatInt(reset.Unix(), 10))
	headers.Set("Anthropic-Ratelimit-Unified-7d-Status", "allowed")

	err := runClaudeStreamWithInStreamError(t, "rate_limit_error", headers, sdktranslator.FromString("claude"))
	if status, ok := err.(interface{ StatusCode() int }); !ok || status.StatusCode() != http.StatusTooManyRequests {
		t.Fatalf("status = %v; want 429", err)
	}
	scoped, ok := err.(interface{ IsCredentialScoped() bool })
	if !ok || !scoped.IsCredentialScoped() {
		t.Fatal("a rejected shared 5h window must cool the whole credential, like an HTTP 429")
	}
	retryAfter, ok := err.(interface{ RetryAfter() *time.Duration })
	if !ok || retryAfter.RetryAfter() == nil {
		t.Fatal("the 5h reset from the stream-start headers must become the cooldown deadline")
	}
	if got := *retryAfter.RetryAfter(); got < 119*time.Minute || got > 2*time.Hour+time.Minute {
		t.Fatalf("RetryAfter = %v; want about 2h", got)
	}

	// Without quota headers the rate limit stays model-scoped with no deadline, as over HTTP.
	err = runClaudeStreamWithInStreamError(t, "rate_limit_error", nil, sdktranslator.FromString("claude"))
	if scoped, ok := err.(interface{ IsCredentialScoped() bool }); !ok || scoped.IsCredentialScoped() {
		t.Fatalf("headerless in-stream rate limit must be model-scoped: %v", err)
	}
}
